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

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

 auto LRUReplacer::Victim(frame_id_t* frame_id) -> bool {
        //WSDB_STUDENT_TODO(l1, t1); 
        std::lock_guard<std::mutex> lock(latch_);
        for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
            if (it->second) { // 可被淘汰
                *frame_id = it->first;
                lru_hash_.erase(it->first);
                lru_list_.erase(--(it.base())); // 删除当前可淘汰的帧
                --cur_size_;
                return true;
            }
        }
        return false; // 没有找到可淘汰的帧
    }

    void LRUReplacer::Pin(frame_id_t frame_id) {  // 固定某个帧，表示该页面被访问
        //WSDB_STUDENT_TODO(l1, t1); 
        std::lock_guard<std::mutex> lock(latch_);
        auto it = lru_hash_.find(frame_id);
        if (it != lru_hash_.end()) { //如果在列表中
            if (it->second->second == true) {//如果是可被淘汰，更新为不可被淘汰
                it->second->second = false;
                cur_size_--;
            }
            lru_list_.erase(it->second);//先删掉，之后再插入头部            
        }
        else { //如果不在列表中
            if (lru_hash_.size() == max_size_) { //缓存区已满
                 for (auto i = lru_list_.rbegin(); i != lru_list_.rend(); ++i) {
                    if (i->second) { // 可被淘汰
                        lru_hash_.erase(i->first);
                        lru_list_.erase(--(i.base())); // 删除当前可淘汰的帧
                        --cur_size_;
                    }
                }
            }
        }
        // 插入到链表头部，表示最近访问且不可被淘汰
        lru_list_.emplace_front(frame_id, false); // 固定的帧不可被淘汰
        lru_hash_[frame_id] = lru_list_.begin();
    }

    void LRUReplacer::Unpin(frame_id_t frame_id) { //取消固定，就可以被淘汰了
        //WSDB_STUDENT_TODO(l1, t1); 
        std::lock_guard<std::mutex> lock(latch_);
        auto it = lru_hash_.find(frame_id);
        if (it != lru_hash_.end()) {
            if (it->second->second == false) {
                it->second->second = true;
                cur_size_++;
            }
        }
    }

    auto LRUReplacer::Size() -> size_t {
        std::lock_guard<std::mutex> lock(latch_);  // 自动锁定
        return cur_size_;
    }

}  // namespace wsdb
