#pragma once

#include <algorithm>
#include <iosfwd>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "sql/column_segment.h"
#include "sql/schema.h"
#include "sql/value.h"
#include "util/common.h"
#include "util/macros.h"

extern i32 current_partition;

namespace tpl::sql {

/**
 * A SQL table. It's stupid and only for testing the system out. It'll be
 * ripped out when we pull it into the full DBMS
 */
class Table {
 public:
  /**
   * A collection of column values forming a block of tuples in the table
   */
  class Block {
   public:
    Block(std::vector<ColumnSegment> &&data, u32 num_tuples)
        : data_(std::move(data)), num_tuples_(num_tuples) {}

    u32 num_cols() const { return static_cast<u32>(data_.size()); }

    u32 num_tuples() const { return num_tuples_; }

    const ColumnSegment *GetColumnData(u32 col_idx) const {
      TPL_ASSERT(col_idx < num_cols(), "Invalid column index!");
      return &data_[col_idx];
    }

   private:
    std::vector<ColumnSegment> data_;
    u32 num_tuples_;
  };

  using BlockList = std::vector<Block>;

  /**
   * Create a new table with ID \ref id and physical layout \ref schema
   * @param id The desired ID of the table
   * @param schema The physical schema of the table
   */
  Table(u16 id, std::unique_ptr<Schema> schema)
      : schema_(std::move(schema)), id_(id), num_tuples_(0) {}

  /**
   * Insert column data from \ref data into the table
   * \param block The block of data to insert into the table
   */
  void Insert(Block &&block);

  /**
   * Return the block at the given index in the table's block list
   */
  Block *GetBlock(const u32 block_idx) {
    TPL_ASSERT(block_idx < blocks_.size(), "Out-of-bounds block access");
    return &blocks_[block_idx];
  }

  /**
   * Iterators over the blocks in the table
   */
  Table::BlockList::const_iterator begin() const { return blocks_.begin(); }
  Table::BlockList::iterator begin() { return blocks_.begin(); }
  Table::BlockList::const_iterator end() const { return blocks_.end(); }
  Table::BlockList::iterator end() { return blocks_.end(); }

  /**
   * Dump the contents of the table to the output stream in CSV format
   * @param os The output stream to write contents into
   */
  void Dump(std::ostream &os) const;

  /**
   * Return the ID of the table
   */
  u16 id() const { return id_; }

  /**
   * Return the total number of tuples in the table
   */
  u32 num_tuples() const { return num_tuples_; }

  /**
   * Return the schema of the table
   */
  const Schema &schema() const { return *schema_; }

  /**
   * Return the number of blocks in the table
   */
  u32 num_blocks() const { return blocks_.size(); }

 private:
  std::unique_ptr<Schema> schema_;
  BlockList blocks_;
  u16 id_;
  u32 num_tuples_;
};

/**
 * An iterator over the blocks in a table
 */
class TableBlockIterator {
 public:
  /**
   * Create an iterator over all the blocks in the table with the given ID
   */
  explicit TableBlockIterator(u16 table_id);

  /**
   * Create an iterator over a subset of the blocks in the table with ID
   * @em table_id. Iteration occurs of the range [start, end).
   * @param table_id The ID of the table
   * @param start_block_idx The index of the block to begin at
   * @param end_block_idx The index of the block to stop at
   */
  TableBlockIterator(u16 table_id, u32 start_block_idx, u32 end_block_idx);

  /**
   * Initialize the iterator returning true if it succeeded
   * @return True if the initialization succeeded; false otherwise
   */
  bool Init();

  /**
   * Advance the iterator to the next block in the table
   * @return True if there is another block in the iterator; false otherwise
   */
  bool Advance();

  /**
   * Return the table this iterator is scanning over
   */
  const Table *table() const { return table_; }

  /**
   * Return the current block
   */
  const Table::Block *current_block() const { return curr_block_; }

 private:
  // The ID of the table to iterate
  u16 table_id_;
  // The index of the block to begin iteration
  u32 start_block_idx_;
  // The index of the block to end iteration
  u32 end_block_idx_;
  // The table we're scanning over
  const Table *table_;
  // The current block
  const Table::Block *curr_block_;
  // The position of the next block in the iteration
  Table::BlockList::const_iterator pos_;
  // The ending position of the iteration
  Table::BlockList::const_iterator end_;
};

}  // namespace tpl::sql
