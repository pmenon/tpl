#include "catalog/catalog.h"
#include <iostream>
#include <memory>
#include <random>
#include "sql/data_types.h"
#include "sql/execution_structures.h"
#include "util/bit_util.h"

namespace terrier::catalog {

using namespace tpl;
Catalog::TableInfo *Catalog::LookupTableByName(const std::string &name) {
  if (name_ids.find(name) == name_ids.end()) return nullptr;
  return LookupTableById(name_ids[name]);
}

Catalog::TableInfo *Catalog::LookupTableByName(tpl::ast::Identifier name) {
  return LookupTableByName(name.data());
}

Catalog::TableInfo *Catalog::LookupTableById(table_oid_t table_id) {
  if (table_catalog_.find(table_id) == table_catalog_.end()) return nullptr;
  return table_catalog_[table_id].get();
}

bool Catalog::CreateTable(const std::string &table_name,
                          Schema &&storage_schema, sql::Schema &&sql_schema) {
  auto *exec = sql::ExecutionStructures::Instance();
  auto table = std::make_unique<storage::SqlTable>(
      exec->GetBlockStore(), storage_schema, table_oid_t(curr_id_));
  table_oid_t oid = GetNewTableOid();
  auto info = std::make_unique<TableInfo>(
      std::move(table), std::move(storage_schema), std::move(sql_schema), oid);
  table_catalog_[oid] = std::move(info);
  name_ids[table_name] = oid;
  return true;
}

Schema::Column Catalog::MakeStorageColumn(const std::string &name,
                                          const sql::Type &sql_type) {
  type::TypeId storage_type;
  switch (sql_type.type_id()) {
    case sql::TypeId::BigInt:
      storage_type = type::TypeId::BIGINT;
      break;
    case sql::TypeId::Boolean:
      storage_type = type::TypeId::BOOLEAN;
      break;
    case sql::TypeId::Integer:
      storage_type = type::TypeId::INTEGER;
      break;
    case sql::TypeId::Char:
      storage_type = type::TypeId::TINYINT;
      break;
    case sql::TypeId::Decimal:
      storage_type = type::TypeId::DECIMAL;
      break;
    case sql::TypeId::SmallInt:
      storage_type = type::TypeId::SMALLINT;
      break;
    case sql::TypeId::Varchar:
      storage_type = type::TypeId::VARCHAR;
      break;
    case sql::TypeId::Date:
      storage_type = type::TypeId::DATE;
      break;
    default:
      storage_type = type::TypeId::INVALID;
  }
  return Schema::Column(name, storage_type, sql_type.nullable(),
                        GetNewColOid());
}

sql::Schema::ColumnInfo Catalog::MakeSqlColumn(const std::string &name,
                                               const sql::Type &sql_type) {
  return sql::Schema::ColumnInfo(name, sql_type);
}

/// Test Tables
/**
 * Enumeration to characterize the distribution of values in a given column
 */
enum class Dist : u8 { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial };

/**
 * Metadata about the data for a given column. Specifically, the type of the
 * column, the distribution of values, a min and max if appropriate.
 */
struct ColumnInsertMeta {
  const char *name;
  const sql::Type &type;
  Dist dist;
  u64 min;
  u64 max;

  ColumnInsertMeta(const char *name, const sql::Type &type, Dist dist, u64 min,
                   u64 max)
      : name(name), type(type), dist(dist), min(min), max(max) {}
};

/**
 * Metadata about data within a table. Specifically, the schema and number of
 * rows in the table.
 */
struct TableInsertMeta {
  const char *name;
  u32 num_rows;
  std::vector<ColumnInsertMeta> col_meta;

  TableInsertMeta(const char *name, u32 num_rows,
                  std::vector<ColumnInsertMeta> col_meta)
      : name(name), num_rows(num_rows), col_meta(std::move(col_meta)) {}
};

/**
 * This array configures each of the test tables. When the catalog is created,
 * it bootstraps itself with the tables in this array. Each able is configured
 * with a name, size, and schema. We also configure the columns of the table. If
 * you add a new table, set it up here.
 */
// clang-format off
TableInsertMeta insert_meta[] = {
    // The empty table
    {"empty_table", 0,
     {{"colA", sql::IntegerType::Instance(false), Dist::Serial, 0, 0}}},

    // Table 1
    {"test_1", test_table_size,
     {{"colA", sql::IntegerType::Instance(false), Dist::Serial, 0, 0},
      {"colB", sql::IntegerType::Instance(false), Dist::Uniform, 0, 9},
      {"colC", sql::IntegerType::Instance(false), Dist::Uniform, 0, 9999},
      {"colD", sql::IntegerType::Instance(false), Dist::Uniform, 0, 99999}}},
};


template <typename T>
T *CreateNumberColumnData(Dist dist, u32 num_vals, u64 min, u64 max) {
  static u64 serial_counter = 0;
  auto *val = static_cast<T *>(malloc(sizeof(T) * num_vals));

  switch (dist) {
    case Dist::Uniform: {
      std::mt19937 generator;
      std::uniform_int_distribution<T> distribution(min, max);

      for (u32 i = 0; i < num_vals; i++) {
        val[i] = distribution(generator);
      }

      break;
    }
    case Dist::Serial: {
      for (u32 i = 0; i < num_vals; i++) {
        val[i] = serial_counter++;
      }
      break;
    }
    default:
      throw std::runtime_error("Implement me!");
  }

  return val;
}

std::pair<byte *, u32*> GenerateColumnData(const ColumnInsertMeta &col_meta,
                                           u32 num_rows) {
  // Create data
  byte *col_data = nullptr;
  switch (col_meta.type.type_id()) {
    case sql::TypeId::Boolean: {
      throw std::runtime_error("Implement me!");
    }
    case sql::TypeId::SmallInt: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i16>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case sql::TypeId::Integer: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i32>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case sql::TypeId::BigInt:
    case sql::TypeId::Decimal: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i64>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case sql::TypeId::Date:
    case sql::TypeId::Char:
    case sql::TypeId::Varchar: {
      throw std::runtime_error("Implement me!");
    }
  }

  // Create bitmap
  u32 *null_bitmap = nullptr;
  TPL_ASSERT(num_rows != 0, "Cannot have 0 rows.");
  u64 num_words = util::BitUtil::Num32BitWordsFor(num_rows);
  null_bitmap = static_cast<u32 *>(malloc(num_words * sizeof(u32)));
  util::BitUtil::Clear(null_bitmap, num_rows);

  return {col_data, null_bitmap};
}

// clang-format on
void FillTable(const TableInsertMeta &table_meta) {
  auto *exec = sql::ExecutionStructures::Instance();

  // Get the Table
  auto *catalog = exec->GetCatalog();
  Catalog::TableInfo *info = catalog->LookupTableByName(table_meta.name);
  storage::SqlTable *table = info->GetTable();
  const Schema *schema = info->GetStorageSchema();
  std::vector<col_oid_t> col_oids;
  for (const auto &col : schema->GetColumns())
    col_oids.emplace_back(col.GetOid());

  // Begin a transactions
  auto *txn_manager = exec->GetTxnManager();
  auto *txn = txn_manager->BeginTransaction();

  // Initilize the insert buffer.
  auto pri = table->InitializerForProjectedRow(col_oids);
  auto *insert_buffer_ =
      common::AllocationUtil::AllocateAligned(pri.first.ProjectedRowSize());
  auto *insert_ = pri.first.InitializeRow(insert_buffer_);

  u32 batch_size = 10000;
  u32 num_batches = table_meta.num_rows / batch_size +
                    static_cast<u32>(table_meta.num_rows % batch_size != 0);
  u32 val_written = 0;
  for (u32 i = 0; i < num_batches; i++) {
    std::vector<std::pair<byte *, u32 *>> column_data;

    // Generate column data for all columns
    u32 num_vals = std::min(batch_size, table_meta.num_rows - (i * batch_size));
    TPL_ASSERT(num_vals != 0, "Can't have empty columns.");
    for (const auto &col_meta : table_meta.col_meta) {
      column_data.emplace_back(GenerateColumnData(col_meta, num_vals));
    }
    for (u32 j = 0; j < num_vals; j++) {
      for (u16 k = 0; k < column_data.size(); k++) {
        if (table_meta.col_meta[k].type.nullable() &&
            util::BitUtil::Test(column_data[k].second, j)) {
          insert_->SetNull(k);
        } else {
          byte *data = insert_->AccessForceNotNull(k);
          u32 data_size = schema->GetColumns()[k].GetAttrSize();
          std::memcpy(data, column_data[k].first + j * data_size, data_size);
          val_written += 1;
        }
      }
      table->Insert(txn, *insert_);
    }
    for (const auto &col_data : column_data) {
      std::free(col_data.first);
      std::free(col_data.second);
    }
  }
  std::cout << "Inserted " << val_written << " tuples in " << table_meta.name
            << std::endl;
  txn_manager->Commit(txn, [](void *) { return; }, nullptr);
}

void Catalog::CreateTestTables() {
  for (const auto &table_meta : insert_meta) {
    std::vector<Schema::Column> storage_cols;
    std::vector<sql::Schema::ColumnInfo> sql_cols;
    for (const auto &col_meta : table_meta.col_meta) {
      storage_cols.emplace_back(
          MakeStorageColumn(col_meta.name, col_meta.type));
      sql_cols.emplace_back(MakeSqlColumn(col_meta.name, col_meta.type));
    }
    Schema storage_schema(std::move(storage_cols));
    sql::Schema sql_schema(std::move(sql_cols));
    CreateTable(table_meta.name, std::move(storage_schema),
                std::move(sql_schema));
    FillTable(table_meta);
  }
};

}  // namespace terrier::catalog
