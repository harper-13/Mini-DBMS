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
 // Created by ziqi on 2024/7/18.
 //

#include "executor_seqscan.h"

namespace wsdb {

  SeqScanExecutor::SeqScanExecutor(TableHandle* tab) : AbstractExecutor(Basic), tab_(tab) {}

  void SeqScanExecutor::Init()
  {
    rid_ = tab_->GetFirstRID();

    //WSDB_STUDENT_TODO(l2, t1);
    //TODO:
    // 初始化 record_ 为第一个有效记录
    if (rid_ != INVALID_RID) {
      auto record = tab_->GetRecord(rid_);
      if (record) {
        record_ = std::make_unique<Record>(*record);
      }
      else {
        record_.reset();
      }
    }
    else {
      record_.reset(); // 如果没有记录，初始化为空
    }
  }

  void SeqScanExecutor::Next() {
    //WSDB_STUDENT_TODO(l2, t1);
    //TODO:
    // 检查当前是否已经结束
    if (rid_ == INVALID_RID) {
      record_.reset();
      return;
    }
    // 获取下一个记录的 RID
    RID next_rid = tab_->GetNextRID(rid_);
    // 如果没有下一个记录，设置为结束状态
    if (next_rid == INVALID_RID) {
      rid_ = INVALID_RID;
      record_.reset();
      return;
    }
    // 更新当前 RID
    rid_ = next_rid;
    // 获取当前记录
    auto record = tab_->GetRecord(rid_);
    if (record) {
      record_ = std::make_unique<Record>(*record);
    }
    else {
      record_.reset();
    }
  }

  auto SeqScanExecutor::IsEnd() const -> bool {
    //WSDB_STUDENT_TODO(l2, t1);
    //TODO:
    return rid_ == INVALID_RID;
  }

  auto SeqScanExecutor::GetOutSchema() const -> const RecordSchema* { return &tab_->GetSchema(); }
}  // namespace wsdb