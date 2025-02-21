
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

#include "executor_update.h"

namespace wsdb {

UpdateExecutor::UpdateExecutor(AbstractExecutorUptr child, TableHandle *tbl, std::list<IndexHandle *> indexes,
    std::vector<std::pair<RTField, ValueSptr>> updates)
    : AbstractExecutor(DML),
      child_(std::move(child)),
      tbl_(tbl),
      indexes_(std::move(indexes)),
      updates_(std::move(updates)),
      is_end_(false)
{
  std::vector<RTField> fields(1);
  fields[0]   = RTField{.field_ = {.field_name_ = "updated", .field_size_ = sizeof(int), .field_type_ = TYPE_INT}};
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void UpdateExecutor::Init() { WSDB_FETAL("UpdateExecutor does not support Init"); }

void UpdateExecutor::Next()
{
  // number of updated records
  int count = 0;

  //WSDB_STUDENT_TODO(l2, t1);
  //TODO:
  // Loop through all the child records
  while (!child_->IsEnd()) {
      // Get the next record from the child executor
      auto child_record = child_->GetRecord();

      if (!child_record) {
          break;
      }

      // Get the RID (Record Identifier) of the record to be updated
      RID rid = child_record->GetRID();

      // Prepare the updated values
      auto  updated_values = child_record->GetSchema()->GetFields();
      for (const auto &[field, value] : updates_) {
          // Find the index of the field to update
          size_t field_index = child_record->GetSchema()->GetFieldIndex(INVALID_TABLE_ID, field.field_.field_name_);
          updated_values[field_index] = value;
      }

      // Create a new updated record
      Record updated_record(tbl_->GetSchema().GetFields(), updated_values, rid);
      // Update the table
      tbl_->UpdateRecord(rid, updated_record);

      // Update the indexes
      for (auto *index : indexes_) {
          index->UpdateRecord(*child_record, updated_record);
      }

      // Increment the count of updated records
      count++;

      // Move to the next record in the child executor
      child_->Next();
  }

  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(count)};
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  is_end_ = true;
}

auto UpdateExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace wsdb