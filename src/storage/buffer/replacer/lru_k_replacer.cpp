/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"

namespace wsdb {

    LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {}



auto LRUKReplacer::Victim(frame_id_t* frame_id) -> bool {
    //WSDB_STUDENT_TODO(labs(), f1);
    std::lock_guard<std::mutex> lock(latch_);
    if (cur_size_ == 0) {
        return false; // 当前没有可淘汰的 frame
    }

    std::vector<frame_id_t> temp_infs;  // 用于存储正无穷 distance 的 frame
    frame_id_t max_distance_frame = INVALID_FRAME_ID;
    unsigned long long max_distance = 0;

    // 遍历 node_store_，获取每个 frame 的 backward k-distance
    for (auto &[fid, node] : node_store_) {
        if (!node.IsEvictable()) {
            continue; // 跳过不可淘汰的 frame
        }

        unsigned long long distance = node.GetBackwardKDistance(cur_ts_);
        if (distance == std::numeric_limits<unsigned long long>::max()) {
            // 如果 distance 为正无穷，将该 frame 加入 temp_infs
            temp_infs.push_back(fid);
        } else {
            // 找到非正无穷中最大的 distance 和对应的 frame
            if (distance > max_distance) {
                max_distance = distance;
                max_distance_frame = fid;
            }
        }
    }

    if (!temp_infs.empty()) {
        // 如果存在正无穷 distance 的 frame，选出第一次访问时间最小的 frame
        frame_id_t earliest_frame = temp_infs[0];
        timestamp_t earliest_time = node_store_[earliest_frame].GetFirstTime();

        for (auto &fid : temp_infs) {
            timestamp_t first_time = node_store_[fid].GetFirstTime();
            if (first_time < earliest_time) {
                earliest_time = first_time;
                earliest_frame = fid;
            }
        }

        // 选中第一次访问时间最小的 frame
        *frame_id = earliest_frame;
        node_store_.erase(earliest_frame);
    } else if (max_distance_frame != INVALID_FRAME_ID) {
        // 如果没有正无穷的 frame，则选择非正无穷中最大 distance 的 frame
        *frame_id = max_distance_frame;
        node_store_.erase(max_distance_frame);
    } else {
        return false; // 无法找到符合条件的 frame
    }

    --cur_size_; // 减少可淘汰 frame 的数量
    return true;
}


    void LRUKReplacer::Pin(frame_id_t frame_id) {
        std::lock_guard<std::mutex> lock(latch_);
        cur_ts_++;
        auto it = node_store_.find(frame_id);
        if (it != node_store_.end()) {
            it->second.SetEvictable(false);
            cur_size_--;            
        }
        else {
            if (node_store_.size() == max_size_) {           
                Victim(&frame_id);
            }
            node_store_[frame_id] = LRUKNode(frame_id, k_);
        }
        node_store_[frame_id].AddHistory(cur_ts_);
    }

void LRUKReplacer::Unpin(frame_id_t frame_id) { //取消固定，就可以被淘汰了
    //WSDB_STUDENT_TODO(l1, t1); 
    std::lock_guard<std::mutex> lock(latch_);
    cur_ts_++;//
    auto it = node_store_.find(frame_id);
    if (it != node_store_.end() && !it->second.IsEvictable()) {
        it->second.SetEvictable(true);
        ++cur_size_; // Increase evictable size
    }
}

auto LRUKReplacer::Size() -> size_t {
    std::lock_guard<std::mutex> lock(latch_);  // 自动锁定
    return cur_size_;
}

}  // namespace wsdb
