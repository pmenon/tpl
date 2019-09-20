#include "sql/tablegen/table_loader.h"

#include <string>
#include <vector>

#include "csv/csv.hpp"

#include "sql/column_segment.h"
#include "sql/table.h"
#include "sql/tablegen/schema_reader.h"
#include "sql/value.h"
#include "util/bit_util.h"
#include "util/memory.h"

namespace tpl::sql::tablegen {

namespace {

// Postgres NULL string
constexpr const char *null_string = "\\N";

void WriteTableCol(byte *data, uint32_t *null_bitmap, const Schema::ColumnInfo &col,
                   uint32_t row_idx, csv::CSVField *field) {
  if (col.sql_type.nullable()) {
    if (*field == null_string) {
      util::BitUtil::Set(null_bitmap, row_idx);
      return;
    }
    util::BitUtil::Unset(null_bitmap, row_idx);
  }

  // Write column data
  byte *insert_offset = data + row_idx * col.GetStorageSize();
  switch (col.sql_type.id()) {
    case SqlTypeId::TinyInt: {
      auto val = field->get<int8_t>();
      std::memcpy(insert_offset, &val, sizeof(int8_t));
      break;
    }
    case SqlTypeId::SmallInt: {
      auto val = field->get<int16_t>();
      std::memcpy(insert_offset, &val, sizeof(int16_t));
      break;
    }
    case SqlTypeId::Integer: {
      auto val = field->get<int32_t>();
      std::memcpy(insert_offset, &val, sizeof(int32_t));
      break;
    }
    case SqlTypeId::BigInt: {
      auto val = field->get<int64_t>();
      std::memcpy(insert_offset, &val, sizeof(int64_t));
      break;
    }
    case SqlTypeId::Real: {
      auto val = field->get<float>();
      std::memcpy(insert_offset, &val, sizeof(float));
      break;
    }
    case SqlTypeId::Double: {
      auto val = field->get<double>();
      std::memcpy(insert_offset, &val, sizeof(double));
      break;
    }
#if 0
    case SqlTypeId::Date: {
      auto val = ValUtil::StringToDate(field->get<std::string>());
      std::memcpy(insert_offset, &val.int_val, sizeof(uint32_t));
      break;
    }
    case SqlTypeId::Varchar: {
      auto val = field->get<std::string_view>();
      auto content_size = static_cast<uint32_t>(val.size() + 1);
      byte *content = static_cast<byte *>(util::MallocAligned(content_size, 8));
      std::memcpy(content, val.data(), content_size - 1);
      content[content_size - 1] = static_cast<byte>(0);
      if (content_size <= VarlenEntry::InlineThreshold()) {
        *reinterpret_cast<VarlenEntry *>(insert_offset) =
            VarlenEntry::CreateInline(content, content_size);
        std::free(content);
      } else {
        // TODO(Amadou): Use execCtx allocator
        *reinterpret_cast<VarlenEntry *>(insert_offset) =
            VarlenEntry::Create(content, content_size, true);
      }
      break;
    }
#endif
    default:
      UNREACHABLE("Unsupported type. Add it here first!!!");
  }
}

}  // namespace

Table *TableLoader::CreateTable(TableInfo *info) {
  auto table_id = catalog_->AllocateTableId();
  auto table = std::make_unique<Table>(table_id, std::make_unique<Schema>(std::move(info->cols)));
  catalog_->InsertTable(info->table_name, std::move(table));
  return catalog_->LookupTableById(table_id);
}

uint32_t TableLoader::LoadTable(const std::string &schema_file, const std::string &data_file) {
  // The size of the column segments we insert into the table
  static constexpr uint32_t kBatchSize = 10000;

  // Read schema and create table and indexes
  SchemaReader schema_reader;
  std::unique_ptr<TableInfo> table_info = schema_reader.ReadTableInfo(schema_file);
  if (table_info == nullptr) {
    return 0;
  }

  Table *table = CreateTable(table_info.get());
  const auto &cols = table->schema().columns();

  std::vector<std::pair<byte *, uint32_t *>> table_data;

  uint32_t val_written = 0;
  uint32_t num_vals = 0;

  csv::CSVFormat format;
  format.delimiter('|');
  csv::CSVReader reader(data_file, format);
  for (csv::CSVRow &row : reader) {
    if (num_vals == 0) {
      for (const auto &col : table->schema().columns()) {
        byte *data = static_cast<byte *>(util::MallocAligned(
            GetTypeIdSize(col.sql_type.GetPrimitiveTypeId()) * kBatchSize, CACHELINE_SIZE));
        uint32_t *nulls = nullptr;
        if (col.sql_type.nullable()) {
          uint64_t num_words = util::BitUtil::Num32BitWordsFor(kBatchSize);
          nulls = static_cast<uint32_t *>(malloc(num_words * sizeof(uint32_t)));
        }
        table_data.emplace_back(data, nulls);
      }
    }

    // Write table data
    uint16_t col_idx = 0;
    for (csv::CSVField &field : row) {
      WriteTableCol(table_data[col_idx].first, table_data[col_idx].second, cols[col_idx], num_vals,
                    &field);
      col_idx++;
    }

    num_vals++;
    if (num_vals == kBatchSize) {
      std::vector<ColumnSegment> columns;
      for (uint32_t i = 0; i < cols.size(); i++) {
        auto [data, null_bitmap] = table_data[i];
        columns.emplace_back(cols[i].sql_type, data, null_bitmap, num_vals);
      }
      // Insert into table
      table->Insert(Table::Block(std::move(columns), num_vals));
      table_data.clear();
      val_written += num_vals;
      num_vals = 0;
    }
  }

  if (num_vals > 0) {
    std::vector<ColumnSegment> columns;
    for (uint64_t i = 0; i < cols.size(); i++) {
      auto [data, null_bitmap] = table_data[i];
      columns.emplace_back(cols[i].sql_type, data, null_bitmap, num_vals);
    }
    // Insert into table
    table->Insert(Table::Block(std::move(columns), num_vals));
    val_written += num_vals;
  }

  // Return
  return val_written;
}

}  // namespace tpl::sql::tablegen
