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

/// A SQL table. It's stupid and only for testing the system out. It'll be
/// ripped out when we pull it into the full DBMS
class Table {
 public:
  /// A collection of column values forming a block of tuples in the table
  class Block {
   public:
    Block(std::vector<ColumnVector> &&data, u32 num_tuples)
        : data_(std::move(data)), num_tuples_(num_tuples) {}

    u32 num_cols() const { return static_cast<u32>(data_.size()); }

    u32 num_tuples() const { return num_tuples_; }

    const ColumnVector *GetColumnData(u32 col_idx) const {
      TPL_ASSERT(col_idx < num_cols(), "Invalid column index!");
      return &data_[col_idx];
    }

   private:
    std::vector<ColumnVector> data_;
    u32 num_tuples_;
  };

  using BlockList = std::vector<Block>;

  /// An iterator over the blocks in a table
  class BlockIterator {
   public:
    bool Advance() {
      if (pos_ == end_) {
        return false;
      }

      curr_block_ = &*pos_;
      ++pos_;
      return true;
    }

    const Block *current_block() const { return curr_block_; }

   private:
    friend class Table;
    BlockIterator(BlockList::const_iterator begin,
                  BlockList::const_iterator end) noexcept
        : curr_block_(nullptr), pos_(begin), end_(end) {}

   private:
    const Block *curr_block_;
    BlockList::const_iterator pos_;
    BlockList::const_iterator end_;
  };

  /// Create a new table with ID \ref id and physical layout \ref schema
  /// \param id The desired ID of the table
  /// \param schema The physical schema of the table
  Table(u16 id, std::unique_ptr<Schema> schema)
      : schema_(std::move(schema)), id_(id), num_tuples_(0) {}

  /// Insert column data from \ref data into the table
  /// \param block The block of data to insert into the table
  void Insert(Block &&block);

  /// Return an iterator over all blocks of the table
  /// \return
  BlockIterator Iterate() const {
    return BlockIterator(blocks().cbegin(), blocks().cend());
  }

  /// Dump the contents of the table to the output stream in CSV format
  /// \param os The output stream to write contents into
  void Dump(std::ostream &os) const;

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  u16 id() const { return id_; }

  u32 num_tuples() const { return num_tuples_; }

  u32 num_columns() const { return schema().NumColumns(); }

  const Schema &schema() const { return *schema_; }

  const BlockList &blocks() const { return blocks_; }

 private:
  std::unique_ptr<Schema> schema_;
  BlockList blocks_;
  u16 id_;
  u32 num_tuples_;
};

}  // namespace tpl::sql