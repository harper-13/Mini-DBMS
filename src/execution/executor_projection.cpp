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

#include "executor_projection.h"

namespace wsdb {

  ProjectionExecutor::ProjectionExecutor(AbstractExecutorUptr child, RecordSchemaUptr proj_schema)
    : AbstractExecutor(Basic), child_(std::move(child))
  {
    out_schema_ = std::move(proj_schema);
  }

  // hint: record_ = std::make_unique<Record>(out_schema_.get(), *child_record);

  void ProjectionExecutor::Init() {
    //WSDB_STUDENT_TODO(l2, t1);
    //TODO:
    // Initialize the child executor
    child_->Init();
  }

  void ProjectionExecutor::Next() {
    //WSDB_STUDENT_TODO(l2, t1);
    //TODO:
    // Fetch the next record from the child executor
    if (child_->IsEnd()) {
      record_.reset(); // No more records
      return;
    }

    auto child_record = child_->GetRecord();
    if (!child_record) {
      record_.reset();
      return;
    }

    // Create a new record based on the projection schema and the child record
    record_ = std::make_unique<Record>(out_schema_.get(), *child_record);
  }


  auto ProjectionExecutor::IsEnd() const -> bool {
    //WSDB_STUDENT_TODO(l2, t1);
    //TODO:
    return child_->IsEnd();
  }

}  // namespace wsdb