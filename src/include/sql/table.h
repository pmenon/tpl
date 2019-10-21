#pragma once

#include <algorithm>
#include <iosfwd>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/common.h"
#include "common/macros.h"
#include "sql/column_segment.h"
#include "sql/runtime_types.h"
#include "sql/schema.h"
#include "sql/value.h"

extern int32_t current_partition;

namespace tpl::sql {

/**
 * A SQL table. It's stupid and only for testing the system out. It'll be ripped out when we pull it
 * into the full DBMS.
 */
class Table {
 public:
  /**
   * A collection of column values forming a block of tuples in the table
   */
  class Block {
   public:
    Block(std::vector<ColumnSegment> &&data, uint32_t num_tuples)
        : data_(std::move(data)), num_tuples_(num_tuples) {}

    uint32_t num_cols() const { return static_cast<uint32_t>(data_.size()); }

    uint32_t num_tuples() const { return num_tuples_; }

    const ColumnSegment *GetColumnData(uint32_t col_idx) const {
      TPL_ASSERT(col_idx < num_cols(), "Invalid column index!");
      return &data_[col_idx];
    }

   private:
    std::vector<ColumnSegment> data_;
    uint32_t num_tuples_;
  };

  /**
   * The container type for all blocks owned by the table
   */
  using BlockList = std::vector<Block>;

  /**
   * Create a new table with ID @em id and physical layout @em schema
   * @param id The desired ID of the table
   * @param schema The physical schema of the table
   */
  Table(uint16_t id, std::unique_ptr<Schema> schema)
      : id_(id), schema_(std::move(schema)), num_tuples_(0) {}

  /**
   * Insert column data from @em data data into the table
   * @param block The block of data to insert into the table
   */
  void Insert(Block &&block);

  /**
   * Return the block at the given index in the table's block list
   */
  Block *GetBlock(const uint32_t block_idx) {
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
   * @param stream The output stream to write contents into
   */
  void Dump(std::ostream &stream) const;

  /**
   * @return The ID of the table.
   */
  uint16_t GetId() const { return id_; }

  /**
   * @return The total number of tuples in the table.
   */
  uint32_t GetTupleCount() const { return num_tuples_; }

  /**
   * @return The schema of the table.
   */
  const Schema &GetSchema() const { return *schema_; }

  /**
   * @return The number of blocks in the table.
   */
  uint32_t GetBlockCount() const { return blocks_.size(); }

  /**
   * @return The mutable string heap for this table.
   */
  VarlenHeap *GetMutableStringHeap() { return &strings_; }

 private:
  // The ID of the table
  uint16_t id_;
  // The table's schema
  std::unique_ptr<Schema> schema_;
  // The list of all blocks constituting the table's data
  BlockList blocks_;
  // Strings
  VarlenHeap strings_;
  // The total number of tuples in the table
  uint32_t num_tuples_;
};

/**
 * An iterator over the blocks in a table
 */
class TableBlockIterator {
 public:
  /**
   * Create an iterator over all the blocks in the table with the given ID
   */
  explicit TableBlockIterator(uint16_t table_id);

  /**
   * Create an iterator over a subset of the blocks in the table with ID @em table_id. Iteration
   * occurs of the range [start, end).
   * @param table_id The ID of the table
   * @param start_block_idx The index of the block to begin at
   * @param end_block_idx The index of the block to stop at
   */
  TableBlockIterator(uint16_t table_id, uint32_t start_block_idx, uint32_t end_block_idx);

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
  const Table *GetTable() const { return table_; }

  /**
   * Return the current block
   */
  const Table::Block *GetCurrentBlock() const { return curr_block_; }

 private:
  // The ID of the table to iterate
  uint16_t table_id_;
  // The index of the block to begin iteration
  uint32_t start_block_idx_;
  // The index of the block to end iteration
  uint32_t end_block_idx_;
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
