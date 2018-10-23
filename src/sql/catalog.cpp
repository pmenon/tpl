#include "sql/catalog.h"

#include <random>
#include <utility>

#include "logging/logger.h"
#include "sql/schema.h"
#include "sql/table.h"
#include "sql/type.h"

namespace tpl::sql {

namespace {

enum class Dist : u8 { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99 };

struct ColumnInsertMeta {
  const char *name;
  Type type;
  Dist dist;
  u64 min;
  u64 max;

  ColumnInsertMeta(const char *name, const Type &type, Dist dist, u64 min,
                   u64 max)
      : name(name), type(type), dist(dist), min(min), max(max) {}
};

struct TableInsertMeta {
  TableId id;
  const char *name;
  u32 num_rows;
  std::vector<ColumnInsertMeta> col_meta;

  TableInsertMeta(TableId id, const char *name, u32 num_rows,
                  std::vector<ColumnInsertMeta> col_meta)
      : id(id), name(name), num_rows(num_rows), col_meta(std::move(col_meta)) {}
};

// clang-format off
std::vector<TableInsertMeta> insert_meta = {
    {TableId::Test1, "test_1", 1000000, {
      {"colA", Type{.type_id = TypeId::Integer, .nullable = false}, Dist::Uniform, 0, std::numeric_limits<i32>::max()},
      {"colB", Type{.type_id = TypeId::Integer, .nullable = false}, Dist::Uniform, 0, std::numeric_limits<i32>::max()},
      {"colC", Type{.type_id = TypeId::Integer, .nullable = false}, Dist::Uniform, 0, std::numeric_limits<i32>::max()},
      {"colD", Type{.type_id = TypeId::Integer, .nullable = false}, Dist::Uniform, 0, std::numeric_limits<i32>::max()}}
    },
};
// clang-format on

template <typename T>
T *CreateNumberColumnData(u32 num_vals, u64 min, u64 max) {
  T *val = static_cast<T *>(malloc(sizeof(T) * num_vals));

  std::default_random_engine generator;
  std::uniform_int_distribution<T> distribution(min, max);

  for (u32 i = 0; i < num_vals; i++) {
    val[i] = distribution(generator);
  }

  return val;
}

std::pair<const byte *, const bool *> GenerateColumnData(
    const ColumnInsertMeta &col_meta, u32 num_vals) {
  // Create data
  byte *col_data = nullptr;
  switch (col_meta.type.type_id) {
    case TypeId::SmallInt: {
      col_data = reinterpret_cast<byte *>(
          CreateNumberColumnData<i16>(num_vals, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::Integer: {
      col_data = reinterpret_cast<byte *>(
          CreateNumberColumnData<i32>(num_vals, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::BigInt:
    case TypeId::Decimal: {
      col_data = reinterpret_cast<byte *>(
          CreateNumberColumnData<i64>(num_vals, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::Varchar: {
      throw std::runtime_error("Implement me!");
    }
  }

  // Create bitmap
  bool *null_bitmap = nullptr;
  if (col_meta.type.nullable) {
    null_bitmap = static_cast<bool *>(malloc(sizeof(bool)));
  }

  return {col_data, null_bitmap};
}

void InitTable(const TableInsertMeta &meta, Table *table) {
  LOG_INFO("Populating table instance '{}' with {} rows", meta.name,
           meta.num_rows);

  u32 batch_size = 1000;
  u32 num_batches =
      meta.num_rows / batch_size + (meta.num_rows % batch_size != 0);

  for (u32 i = 0; i < num_batches; i++) {
    // The column data we'll insert
    std::vector<Table::ColumnVector> columns;
    columns.resize(meta.col_meta.size());

    // Generate column data for all columns
    u32 size = std::min(batch_size, meta.num_rows - (i * batch_size));
    for (u32 col_idx = 0; col_idx < meta.col_meta.size(); col_idx++) {
      const auto &[data, null_bitmap] =
          GenerateColumnData(meta.col_meta[col_idx], size);
      columns[col_idx].data = data;
      columns[col_idx].null_bitmap = null_bitmap;
    }

    // Insert into table
    table->BulkInsert(std::move(columns), size);
  }
}

}  // namespace

Catalog::Catalog() {
  LOG_INFO("Initializing catalog");

  // Insert tables into catalog
  for (const auto &meta : insert_meta) {
    LOG_INFO("Creating table instance '{}' in catalog", meta.name);

    std::vector<Schema::ColInfo> cols;
    for (const auto &col_meta : meta.col_meta) {
      cols.emplace_back(
          Schema::ColInfo{.name = col_meta.name, .type = col_meta.type});
    }

    // Insert into catalog
    table_catalog_[TableId::Test1] = std::make_unique<Table>(
        static_cast<u16>(TableId::Test1), Schema(std::move(cols)));
  }

  // Populate all tables
  for (const auto &table_meta : insert_meta) {
    InitTable(table_meta, LookupTableById(table_meta.id));
  }

  LOG_INFO("Catalog initialization complete");
}

Catalog::~Catalog() = default;

Table *Catalog::LookupTableByName(const std::string &name) {
  static std::unordered_map<std::string, TableId> kTableNameMap = {
#define ENTRY(Name, Str, ...) {Str, TableId::Name},
      TABLES(ENTRY)
#undef ENTRY
  };

  auto iter = kTableNameMap.find(name);
  if (iter == kTableNameMap.end()) {
    return nullptr;
  }

  return LookupTableById(iter->second);
}

Table *Catalog::LookupTableByName(const ast::Identifier name) {
  return LookupTableByName(name.data());
}

Table *Catalog::LookupTableById(TableId table_id) {
  auto iter = table_catalog_.find(table_id);
  return (iter == table_catalog_.end() ? nullptr : iter->second.get());
}

}  // namespace tpl::sql