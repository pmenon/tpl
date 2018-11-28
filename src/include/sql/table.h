#pragma once

#include <algorithm>
#include <iosfwd>
#include <vector>

#include "sql/column.h"
#include "sql/schema.h"
#include "sql/value.h"
#include "util/common.h"

extern i32 current_partition;

namespace tpl::sql {

class Table;
class TableIterator;

/**
 * A SQL table
 */
class Table {
 public:
  /**
   * Create a new table with ID @ref id and physical layout @ref schema
   * @param id The desired ID of the table
   * @param schema The physical schema of the table
   */
  Table(u16 id, Schema &&schema) : id_(id), schema_(std::move(schema)) {}

  /**
   * Insert column data from @data into the table.
   * @param data
   * @param num_rows
   */
  void BulkInsert(std::vector<ColumnVector> &&data, u32 num_rows);

  /**
   * Continue the scan of this table from the given iterators position. If the
   * position is exhausted, this function returns false. Otherwise, the iterator
   * is modified to point to the next row in the table.
   *
   * @param iter The current iterator (i.e., position) in the table
   * @return True if there is more data.
   */
  bool Scan(TableIterator *iter) const;

  /**
   * Dump the contents of the table to the output stream in CSV format
   * @param os The output stream to write contents into
   */
  void Dump(std::ostream &os) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

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
      : table_(table),
        block_(static_cast<u32>(std::max(current_partition, 0))),
        pos_(0),
        bound_(0) {}

  /**
   * Move to the next row in the table.
   *
   * @return True if there is more data; false otherwise.
   */
  bool Next();

  /**
   * Read an integer column value from the current iterator position
   *
   * @tparam type_id The SQL type of the column
   * @tparam nullable Whether the column is NULLable
   * @param col_idx The ID (offset) of the column to read from
   * @param out The output value to populate
   */
  template <TypeId type_id, bool nullable>
  void ReadIntegerColumn(u32 col_idx, Integer *out) const {
    const ColumnVector *col = cols_[col_idx];

    // Set null (if column is nullable)
    if constexpr (nullable) {
      out->null = col->IsNullAt(pos());
    }

    // Set appropriate value
    if constexpr (type_id == TypeId::SmallInt) {
      out->val.smallint = col->GetAt<i16>(pos());
    } else if constexpr (type_id == TypeId::Integer) {
      out->val.integer = col->GetAt<i32>(pos());
    } else if constexpr (type_id == TypeId::BigInt) {
      out->val.bigint = col->GetAt<i64>(pos());
    }
  }

  template <bool nullable>
  void ReadDecimalColumn(u32 col_idx, Decimal *out) const {}

  u32 pos() const { return pos_; }

 private:
  friend class Table;

  Table *table_;
  u32 block_;
  u32 pos_;
  u32 bound_;
  std::vector<const ColumnVector *> cols_;
};

}  // namespace tpl::sql