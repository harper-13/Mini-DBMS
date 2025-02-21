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
        return false; // ��ǰû�п���̭�� frame
    }

    std::vector<frame_id_t> temp_infs;  // ���ڴ洢������ distance �� frame
    frame_id_t max_distance_frame = INVALID_FRAME_ID;
    unsigned long long max_distance = 0;

    // ���� node_store_����ȡÿ�� frame �� backward k-distance
    for (auto &[fid, node] : node_store_) {
        if (!node.IsEvictable()) {
            continue; // ����������̭�� frame
        }

        unsigned long long distance = node.GetBackwardKDistance(cur_ts_);
        if (distance == std::numeric_limits<unsigned long long>::max()) {
            // ��� distance Ϊ��������� frame ���� temp_infs
            temp_infs.push_back(fid);
        } else {
            // �ҵ��������������� distance �Ͷ�Ӧ�� frame
            if (distance > max_distance) {
                max_distance = distance;
                max_distance_frame = fid;
            }
        }
    }

    if (!temp_infs.empty()) {
        // ������������� distance �� frame��ѡ����һ�η���ʱ����С�� frame
        frame_id_t earliest_frame = temp_infs[0];
        timestamp_t earliest_time = node_store_[earliest_frame].GetFirstTime();

        for (auto &fid : temp_infs) {
            timestamp_t first_time = node_store_[fid].GetFirstTime();
            if (first_time < earliest_time) {
                earliest_time = first_time;
                earliest_frame = fid;
            }
        }

        // ѡ�е�һ�η���ʱ����С�� frame
        *frame_id = earliest_frame;
        node_store_.erase(earliest_frame);
    } else if (max_distance_frame != INVALID_FRAME_ID) {
        // ���û��������� frame����ѡ�������������� distance �� frame
        *frame_id = max_distance_frame;
        node_store_.erase(max_distance_frame);
    } else {
        return false; // �޷��ҵ����������� frame
    }

    --cur_size_; // ���ٿ���̭ frame ������
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

void LRUKReplacer::Unpin(frame_id_t frame_id) { //ȡ���̶����Ϳ��Ա���̭��
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
    std::lock_guard<std::mutex> lock(latch_);  // �Զ�����
    return cur_size_;
}

}  // namespace wsdb
