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

#include "executor_insert.h"

namespace wsdb {

  InsertExecutor::InsertExecutor(TableHandle* tbl, std::list<IndexHandle*> indexes, std::vector<RecordUptr> inserts)
    : AbstractExecutor(DML), tbl_(tbl), indexes_(std::move(indexes)), inserts_(std::move(inserts)), is_end_(false)
  {
    std::vector<RTField> fields(1);
    fields[0] = RTField{ .field_ = {.field_name_ = "inserted", .field_size_ = sizeof(int), .field_type_ = TYPE_INT} };
    out_schema_ = std::make_unique<RecordSchema>(fields);
  }

  void InsertExecutor::Init() { WSDB_FETAL("InsertExecutor does not support Init"); }

  void InsertExecutor::Next()
  {
    // number of inserted records
    int count = 0;

    //TODO:
    // Number of successfully inserted records
    int count = 0;

    // Iterate through the list of records to be inserted
    for (auto& record : inserts_) {
      // Step 1: Insert the record into the table
      RID new_rid = tbl_->InsertRecord(*record); // Insert the record into the table and get its RID
      if (new_rid == INVALID_RID) {
        WSDB_THROW(WSDB_RECORD_EXISTS, "Failed to insert record into table");
      }

      // Step 2: Update the RID in the record
      record->SetRID(new_rid);

      // Step 3: Insert the record into all associated indexes
      for (auto& index_handle : indexes_) {
        try {
          // Get the key schema for the index
          const RecordSchema& key_schema = index_handle->GetKeySchema();
          // Generate the index entry key using the current record
          Record key_record(&key_schema, *record);
          // Insert the key record into the index
          index_handle->InsertRecord(key_record);
        }
        catch (const std::exception& e) {
          // Handle index insertion failure
          WSDB_THROW(WSDB_RECORD_EXISTS,
            "Failed to insert record into index: " + index_handle->GetIndexName());
        }
      }

      // Increment the count of successfully inserted records
      count++;
    }

    // Prepare the result record with the number of inserted records
    std::vector<ValueSptr> values{ ValueFactory::CreateIntValue(count) };
    record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
    is_end_ = true;
  }

  auto InsertExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace wsdb
