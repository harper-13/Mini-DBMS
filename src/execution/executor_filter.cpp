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

#include "executor_filter.h"

namespace wsdb {

    FilterExecutor::FilterExecutor(AbstractExecutorUptr child, std::function<bool(const Record&)> filter)
        : AbstractExecutor(Basic), child_(std::move(child)), filter_(std::move(filter))
    {
    }
    void FilterExecutor::Init() {
        //WSDB_STUDENT_TODO(l2, t1);
        //TODO:
        child_->Init();
    }

    void FilterExecutor::Next() {
        //WSDB_STUDENT_TODO(l2, t1);
        //TODO:
        // Loop until we find a record that matches the filter
        while (!child_->IsEnd()) {
            // Get the next record from the child executor
            child_->Next();
            auto current_record = child_->GetRecord();
            // Check if the record passes the filter condition
            if (current_record && filter_(*current_record)) {
                record_ = std::make_unique<Record>(*current_record);
                return;
            }
        }
        // If no matching record is found, reset the output record
        record_.reset();
    }

    auto FilterExecutor::IsEnd() const -> bool {
        //WSDB_STUDENT_TODO(l2, t1);
        //TODO:
        return child_->IsEnd();
    }

    auto FilterExecutor::GetOutSchema() const -> const RecordSchema* { return child_->GetOutSchema(); }
}  // namespace wsdb
