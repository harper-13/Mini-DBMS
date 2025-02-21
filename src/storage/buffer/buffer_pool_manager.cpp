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
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace wsdb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    WSDB_FETAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page * {
    std::lock_guard<std::mutex> lock(latch_);
    fid_pid_t fid_pid = { fid, pid };
    Frame* frame;
    //检查页面是否已经在缓冲区中
    auto it = page_frame_lookup_.find(fid_pid);
    //若页面在缓冲区中
    if (it != page_frame_lookup_.end()) {
      // 如果页面在缓冲区中，获取 frame_id 并将其固定在缓冲区和替换器中
      frame_id_t frame_id = it->second;
      frame = &frames_[frame_id];
      frame->Pin();
      replacer_->Pin(frame_id);  // 固定页面，防止被替换   
      for(auto it = free_list_.begin(); it != free_list_.end(); it++) {
        if(*it == frame_id) {
          free_list_.erase(it);
          break;
        }
      }
      return frame->GetPage(); // 返回该页面的指针
    }
    //页面不在缓冲区中，需要获取可用帧并更新内容
    frame_id_t frame_id = GetAvailableFrame();   
    UpdateFrame(frame_id, fid, pid);// 更新帧内容并加载新页面
    frame = &frames_[frame_id];
    page_frame_lookup_[fid_pid] = frame_id;
    return frame->GetPage();  // 返回页面的指针
 
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    fid_pid_t fid_pid = {fid, pid};
    //检查页面是否已经在缓冲区中
    auto it = page_frame_lookup_.find(fid_pid);
    if (it == page_frame_lookup_.end()) {
      return false; // 页面已不在缓冲区中
    }
    frame_id_t frame_id = it->second;
    Frame& frame = frames_[frame_id];
    //未被使用，不能unpin
    if (frame.InUse() == false) {
      return false;
    }
    frame.Unpin();
    // 如果帧未被使用，将其加入到 replacer 中
    if (frame.InUse() == false) {
      free_list_.push_back(frame_id);   
    }
    replacer_->Unpin(frame_id);
    frame.SetDirty(is_dirty);// 设置脏位
    return true;

}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool {
    std::lock_guard<std::mutex> guard(latch_);
    fid_pid_t fid_pid = {fid, pid};
    //检查页面是否已经在缓冲区中
    auto it = page_frame_lookup_.find(fid_pid);
    if (it == page_frame_lookup_.end()) {
      return true; // 页面已不在缓冲区中
    }
    frame_id_t frame_id = it->second;
    Frame& frame = frames_[frame_id];
    //被使用，不能删除
    if (frame.InUse()) {
      return false;
    }
    // 如果页面被设置了脏位，将页面数据写回磁盘
    if (frame.IsDirty()) {
      const char* data_ptr = frame.GetPage()->GetData();
      disk_manager_->WritePage(fid, pid, data_ptr);  // 写回磁盘
    }
    frame.Reset();// 重置帧内容 
    replacer_->Unpin(frame_id);// 将帧从 replacer 中移除 
    page_frame_lookup_.erase(fid_pid);// 从 page_frame_lookup_ 中删除页面的映射
    return true;

}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool {
  bool res = true;
  std::vector<page_id_t> pid_del; 
  for(auto it : page_frame_lookup_) {
    if(it.first.fid == fid) {
      pid_del.push_back(it.first.pid);
    }
  }  

  for(auto pid : pid_del) {
    res &= DeletePage(fid, pid);
  }

  return res;
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    fid_pid_t fid_pid = {fid, pid};

    //检查页面是否已经在缓冲区中
    auto it = page_frame_lookup_.find(fid_pid);
    if (it == page_frame_lookup_.end()) {
      return false; // 页面已不在缓冲区中
    }
    frame_id_t frame_id = it->second;
    Frame &frame = frames_[frame_id];
    // 如果页面被设置了脏位，将页面数据写回磁盘
    if (frame.IsDirty()) {
        const char* data_ptr = frame.GetPage()->GetData();
        disk_manager_->WritePage(fid, pid, data_ptr);  // 写回磁盘
        frame.SetDirty(false);                        // 清除脏位
    }
    return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool {
  bool res = true;
  for(auto it : page_frame_lookup_) {
    if(it.first.fid == fid) {
      res &= FlushPage(fid, it.first.fid);
    }
  } 
  return res;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t {
    frame_id_t frame_id;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
      return frame_id;
    }
    //如果没有空闲帧，则从 replacer_ 获取一个帧进行替换    
    if (replacer_->Victim(&frame_id)) 
    {
      free_list_.push_front(frame_id);
      free_list_.pop_front();
      return frame_id;
    }
    else 
      // 如果 replacer_ 也无法提供可替换的帧，抛出异常
      throw WSDBException_(WSDB_NO_FREE_FRAME);


}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) {
    fid_pid_t fid_pid = {fid, pid};
    Frame& frame = frames_[frame_id];
    fid_pid_t fid_pid_pre = {frame.GetPage()->GetFileId(), frame.GetPage()->GetPageId()};
    // 如果帧被设置了脏位，将其写回磁盘
    if (frame.IsDirty()) {
      disk_manager_->WritePage(frame.GetPage()->GetFileId(), frame.GetPage()->GetPageId(), frame.GetPage()->GetData());
    }
    frame.GetPage()->Clear();// 清除帧内容 
    frame.GetPage()->SetFilePageId(fid, pid);// 更新页面的 table 和 page ID  
    disk_manager_->ReadPage(fid, pid, frame.GetPage()->GetData());    
    frame.Pin();                // 增加 pin count
    replacer_->Pin(frame_id);   // 在 replacer 中固定  
    for(auto it = free_list_.begin(); it != free_list_.end(); it++) {
      if(*it == frame_id) {
        free_list_.erase(it);
        break;
      }
    }
    page_frame_lookup_.erase(fid_pid_pre);
    page_frame_lookup_[fid_pid] = frame_id;// 更新映射关系
}


auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame * {
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

} // namespace wsdb
