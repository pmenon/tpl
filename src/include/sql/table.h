#pragma once

#include <algorithm>
#include <iosfwd>
#include <memory>
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
   * A collection of column values forming a block of rows in the table
   */
  class Block {
   public:
    Block(std::vector<ColumnVector> &&data, u32 num_rows)
        : data_(std::move(data)), num_rows_(num_rows) {}

    u32 num_cols() const { return static_cast<u32>(data_.size()); }
    u32 num_rows() const { return num_rows_; }

    const ColumnVector &GetColumnData(u32 col_idx) const {
      TPL_ASSERT(col_idx < num_cols(), "Invalid column index!");
      return data_[col_idx];
    }

   private:
    std::vector<ColumnVector> data_;
    u32 num_rows_;
  };

  using BlockList = std::vector<Block>;

  /**
   * An iterator over the blocks in a table
   */
  class BlockIterator {
    BlockIterator &operator++() noexcept {
      ++pos_;
      return *this;
    }

    bool operator==(const BlockIterator &other) const noexcept {
      return pos_ == other.pos_;
    }

    bool operator!=(const BlockIterator &other) const noexcept {
      return !(*this == other);
    }

    const Block *operator*() const { return (pos_ != end_ ? &*pos_ : nullptr); }

   private:
    friend class Table;
    BlockIterator(BlockList::const_iterator begin,
                  BlockList::const_iterator end)
        : pos_(begin), end_(end) {}

   private:
    BlockList::const_iterator pos_;
    BlockList::const_iterator end_;
  };

  /**
   * Create a new table with ID @ref id and physical layout @ref schema
   * @param id The desired ID of the table
   * @param schema The physical schema of the table
   */
  Table(u16 id, std::unique_ptr<Schema> schema)
      : id_(id), schema_(std::move(schema)) {}

  /**
   * Insert column data from @data into the table.
   * @param data
   * @param num_rows
   */
  void Insert(Block &&block);

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
   * Return an iterator over all blocks of the table
   * @return
   */
  BlockIterator begin() const {
    return BlockIterator(blocks().cbegin(), blocks().cend());
  }

  /**
   * Return an iterator pointing to the end of the table
   * @return
   */
  BlockIterator end() const {
    return BlockIterator(blocks().cend(), blocks().cend());
  }

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

  const Schema &schema() const { return *schema_; }

  const BlockList &blocks() const { return blocks_; }

 private:
  u16 id_;
  std::unique_ptr<Schema> schema_;
  BlockList blocks_;
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