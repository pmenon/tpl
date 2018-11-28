#pragma once

#include <iosfwd>
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
  /**
   * A column vector represents a compact array of column values along with a
   * compact, positionally aligned bitmap indicating whether a column value is
   * NULL.
   */
  class ColumnVector {
   public:
    ColumnVector() noexcept : data_(nullptr), null_bitmap_(nullptr) {}
    ColumnVector(const byte *data, const bool *null_bitmap) noexcept
        : data_(data), null_bitmap_(null_bitmap) {}

    ColumnVector(ColumnVector &&other) noexcept
        : data_(other.data_), null_bitmap_(other.null_bitmap_) {
      other.data_ = nullptr;
      other.null_bitmap_ = nullptr;
    }

    DISALLOW_COPY(ColumnVector);

    ~ColumnVector() {
      if (data_ != nullptr) {
        std::free((void *)data_);
        std::free((void *)null_bitmap_);
      }
    }

    template <typename T>
    const T &GetAt(u32 index) const {
      return reinterpret_cast<const T *>(data_)[index];
    }

    bool GetNullAt(u32 index) const { return null_bitmap_[index]; }

   private:
    const byte *data_;
    const bool *null_bitmap_;
  };

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
      : table_(table), block_(0), pos_(0), bound_(0) {}

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
  void GetIntegerColumn(u32 col_idx, Integer *out) const {
    const Table::ColumnVector *col = cols_[col_idx];

    // Set null (if column is nullable)
    if constexpr (nullable) {
      out->null = col->GetNullAt(pos());
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