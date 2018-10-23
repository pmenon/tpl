#pragma once

#include <vector>

#include "sql/schema.h"
#include "sql/value.h"
#include "util/common.h"

namespace tpl::sql {

class Table;
class TableIterator;

/**
 * A SQL table
 */
class Table {
 public:
  struct ColumnVector {
    const byte *data;
    const bool *null_bitmap;

    ColumnVector() : data(nullptr), null_bitmap(nullptr) {}
    ColumnVector(const byte *data, const bool *null_bitmap)
        : data(data), null_bitmap(null_bitmap) {}

    ~ColumnVector() {
      if (data != nullptr) {
        std::free((void *)data);
        std::free((void *)null_bitmap);
      }
    }
  };

  Table(u16 id, Schema &&schema) : id_(id), schema_(std::move(schema)) {}

  void BulkInsert(std::vector<ColumnVector> &&data, u32 num_rows);

  bool Scan(TableIterator *iter) const;

  u16 id() const { return id_; }

  const Schema &schema() const { return schema_; }

 private:
  struct Block {
    std::vector<ColumnVector> columns;
    u32 num_rows;

    Block(std::vector<ColumnVector> &&columns, u32 num_rows)
        : columns(std::move(columns)), num_rows(num_rows) {}
  };

 private:
  u16 id_;
  Schema schema_;
  std::vector<Block> blocks_;
};

/**
 * An iterator over SQL tables
 */
class TableIterator {
 public:
  explicit TableIterator(Table *table)
      : table_(table), block_(0), pos_(0), bound_(0) {
    table->Scan(this);
  }

  bool Next();

  // Read a column value from the current iterator position
  template <TypeId type_id, bool nullable>
  void GetIntegerColumn(u32 col_idx, Integer *out) const {
    const Table::ColumnVector *col = cols_[col_idx];

    // Set null (if column is nullable)
    if constexpr (nullable) {
      out->null = col->null_bitmap[pos()];
    }

    // Set appropriate value
    if constexpr (type_id == TypeId::SmallInt) {
      out->val.smallint = reinterpret_cast<const i16 *>(col->data)[pos()];
    } else if constexpr (type_id == TypeId::Integer) {
      out->val.integer = reinterpret_cast<const i32 *>(col->data)[pos()];
    } else if constexpr (type_id == TypeId::BigInt) {
      out->val.bigint = reinterpret_cast<const i64 *>(col->data)[pos()];
    }
  }

  template <bool nullable>
  void GetDecimalColumn(u32 col_idx, Decimal *out) const {}

  u32 pos() const { return pos_; }

 private:
  friend class Table;

  Table *table_;
  u32 block_;
  u32 pos_;
  u32 bound_;
  std::vector<const Table::ColumnVector *> cols_;
};

}  // namespace tpl::sql