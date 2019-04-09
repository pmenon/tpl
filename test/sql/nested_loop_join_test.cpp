#include "tpl_test.h"

#include <iomanip>
#include <iostream>
#include <random>
#include "sql/data_types.h"
#include "sql/nested_loop_join.h"
#include "sql/schema.h"
#include "util/bit_util.h"

namespace tpl::sql::test {

// Represents a generic tuple with two columns
struct Tuple {
  bool null1, null2;
  i64 col1, col2;
};

class NestedLoopJoinTest : public TplTest {
 public:
  NestedLoopJoinTest()
      : rng_elems{std::numeric_limits<u32>::min(),
                  std::numeric_limits<u32>::max()},
        null_coin{0.5},
        col_a{"col_a", sql::BigIntType::InstanceNullable()},
        col_b{"col_b", sql::BigIntType::InstanceNullable()},
        col_c{"col_c", sql::BigIntType::InstanceNullable()} {}

  /**
   * Run a single test.
   */
  void RunTest() {
    // Generate the number of blocks and tuples
    std::uniform_int_distribution<u32> gen_num_blocks{1, max_num_blocks};
    std::uniform_int_distribution<u32> gen_num_tuples{1,
                                                      max_num_tuples_per_block};
    u32 num_blocks1 = gen_num_blocks(generator_);
    u32 num_blocks2 = gen_num_blocks(generator_);
    u32 num_tuples_per_block1 = gen_num_tuples(generator_);
    u32 num_tuples_per_block2 = gen_num_tuples(generator_);

    // Initialize two table: table2 has a "foreign key" to table1's col_a.
    auto table_schema1 = std::make_unique<sql::Schema>(
        std::vector<sql::Schema::ColumnInfo>{col_a, col_b});
    auto table_schema2 = std::make_unique<sql::Schema>(
        std::vector<sql::Schema::ColumnInfo>{col_a, col_c});
    sql::Table table1{id1, std::move(table_schema1)};
    sql::Table table2{id2, std::move(table_schema2)};

    // Fill up the tables
    // all_data1 and all_data2 contain the same data as each table
    std::vector<Tuple> all_data1;
    std::vector<Tuple> all_data2;
    GenTable(&all_data1, &table1, num_blocks1, num_tuples_per_block1, nullptr);
    GenTable(&all_data2, &table2, num_blocks2, num_tuples_per_block2,
             &all_data1);
    std::vector<std::pair<Tuple, Tuple>> reference_join;
    StdVectorJoin(all_data1, all_data2, &reference_join);

    // Perform the nested loop join and iterate through it
    // Join condition: check that the first column is the same in both rows.
    auto match_fn = [](VectorProjectionIterator *vpi1,
                       VectorProjectionIterator *vpi2) -> bool {
      bool null1, null2;
      const auto *col1_a = vpi1->Get<i64, true>(0, &null1);
      if (null1) return false;
      const auto *col2_a = vpi2->Get<i64, true>(0, &null2);
      if (null2) return false;
      return *col1_a == *col2_a;
    };

    NestedLoopJoin joiner(table1, table2, match_fn);
    NestedLoopJoin::Iterator iter = joiner.Iterate();
    u64 curr_idx = 0;
    while (!iter.Ended()) {
      // Make sure that the iterator does not go over bounds
      ASSERT_TRUE(curr_idx < reference_join.size());
      // Get the current match
      std::pair<VectorProjectionIterator *, VectorProjectionIterator *> match =
          iter.GetCurrMatch();
      // Check that it's the same as the reference
      CompareMatch(reference_join[curr_idx], match);
      curr_idx++;
      iter.Advance();
    }
    ASSERT_EQ(curr_idx, reference_join.size());
  }

  /**
   * Fills up table.
   * @param all_data is list of the generated Tuples.
   * @param table is the table to fill up.
   * @param num_blocks is the number of blocks to create.
   * @param tuples_per_block is the number of tuples per block.
   * @param primary_data: when not null, is used to create foreign key
   * references.
   */
  void GenTable(std::vector<Tuple> *all_data, sql::Table *table,
                const u32 num_blocks, const u32 tuples_per_block,
                const std::vector<Tuple> *primary_data) {
    for (u64 i = 0; i < num_blocks; i++) {
      // Allocate the block data (values and null bitvector).
      auto col1_data = new i64[tuples_per_block];
      auto col2_data = new i64[tuples_per_block];
      auto col1_nulls =
          new u32[util::BitUtil::Num32BitWordsFor(tuples_per_block)];
      auto col2_nulls =
          new u32[util::BitUtil::Num32BitWordsFor(tuples_per_block)];
      util::BitVector col1_bitvector(col1_nulls, tuples_per_block);
      util::BitVector col2_bitvector(col2_nulls, tuples_per_block);
      col1_bitvector.ClearAll();
      col2_bitvector.ClearAll();

      // Used to create random foreign key references.
      u32 index_range = (primary_data == nullptr)
                            ? 0
                            : static_cast<u32>(primary_data->size() - 1);
      std::uniform_int_distribution<u32> rng_index{0, index_range};

      for (u64 j = 0; j < tuples_per_block; j++) {
        if (primary_data != nullptr && !primary_data->empty() &&
            foreign_coin(generator_)) {
          // Create foreign key
          auto idx = rng_index(generator_);
          Tuple tuple = (*primary_data)[idx];
          // Reuse the first column in the tuple to create a foreign key.
          col1_data[j] = tuple.col1;
          // The second column can be modified.
          col2_data[j] = rng_elems(generator_);
          tuple.col2 = col2_data[j];
          tuple.null2 = false;
          tuple.null1 = false;
          all_data->push_back(tuple);
        } else {
          // Create a random tuple
          auto col1_elem = rng_elems(generator_);
          auto col2_elem = rng_elems(generator_);
          auto null1 = null_coin(generator_);
          auto null2 = null_coin(generator_);
          col1_data[j] = col1_elem;
          col2_data[j] = col2_elem;
          if (null1) {
            col1_bitvector.Set(j);
          }
          if (null2) {
            col2_bitvector.Set(j);
          }
          all_data->push_back({null1, null2, col1_elem, col2_elem});
        }
      }
      // Create a Block object and insert it into the table.
      sql::ColumnVector col1(sql::BigIntType::InstanceNullable(),
                             reinterpret_cast<byte *>(col1_data),
                             col1_bitvector.bits(), tuples_per_block);
      sql::ColumnVector col2(sql::BigIntType::InstanceNullable(),
                             reinterpret_cast<byte *>(col2_data),
                             col2_bitvector.bits(), tuples_per_block);
      std::vector<ColumnVector> block_data;
      block_data.emplace_back(std::move(col1));
      block_data.emplace_back(std::move(col2));
      sql::Table::Block block(std::move(block_data), tuples_per_block);
      table->Insert(std::move(block));
    }
  }

  /**
   * Dumps a table to std out. Only used for debugging.
   * @param table is the table to dump.
   */
  void DumpStdTable(const std::vector<Tuple> &table) {
    for (const auto &t : table) {
      if (t.null1)
        std::cout << "NULL";
      else
        std::cout << t.col1;
      std::cout << ", ";

      if (t.null2)
        std::cout << "NULL" << std::endl;
      else
        std::cout << t.col2 << std::endl;
    }
  }

  /**
   * Performs a simple join on two std vectors.
   * @param v1 is the first vector.
   * @param v2 is the second vector.
   * @param output is the output vector.
   */
  void StdVectorJoin(const std::vector<Tuple> &v1, const std::vector<Tuple> &v2,
                     std::vector<std::pair<Tuple, Tuple>> *output) {
    for (const auto &t1 : v1) {
      for (const auto &t2 : v2) {
        if (t1.null1 || t2.null1) continue;
        if (t1.col1 == t2.col1)
          output->emplace_back(std::pair<Tuple, Tuple>{t1, t2});
      }
    }
  }

  /**
   * Checks that the reference match and the returned match are the same.
   * @param reference is the reference match.
   * @param match is the returned match.
   */
  void CompareMatch(const std::pair<Tuple, Tuple> &reference,
                    const std::pair<VectorProjectionIterator *,
                                    VectorProjectionIterator *> &match) {
    bool is_null;
    // Column 1 of the first tuple
    const auto *val = match.first->Get<i64, true>(0, &is_null);
    ASSERT_FALSE(is_null);
    ASSERT_EQ(*val, reference.first.col1);

    // Column 1 of the second tuple
    val = match.second->Get<i64, true>(0, &is_null);
    ASSERT_FALSE(is_null);
    ASSERT_EQ(*val, reference.second.col1);

    // Column 2 of the first tuple
    val = match.first->Get<i64, true>(1, &is_null);
    ASSERT_EQ(is_null, reference.first.null2);
    if (!is_null) {
      ASSERT_EQ(*val, reference.first.col2);
    }

    // Column 2 of the second tuple
    val = match.second->Get<i64, true>(1, &is_null);
    ASSERT_EQ(is_null, reference.second.null2);
    if (!is_null) {
      ASSERT_EQ(*val, reference.second.col2);
    }
  }

 private:
  // Id of each table
  u16 id1{100};
  u16 id2{101};

  std::default_random_engine generator_;
  // Random element generator
  std::uniform_int_distribution<u32> rng_elems;
  // Coin that decides whether an row should point to foreign key
  std::bernoulli_distribution foreign_coin;
  // Coin that decides whether an elem is set to null
  std::bernoulli_distribution null_coin;
  // Maximum number of blocks
  u32 max_num_blocks{16};
  // Maximum number of tuples in each block
  u32 max_num_tuples_per_block{256};
  // Schemas
  sql::Schema::ColumnInfo col_a;
  sql::Schema::ColumnInfo col_b;
  sql::Schema::ColumnInfo col_c;
};

TEST_F(NestedLoopJoinTest, JoinTest) {
  // Use small numbers because the join is slow.
  const u32 num_iterations = 10;
  for (u32 i = 0; i < num_iterations; i++) RunTest();
}
}  // namespace tpl::sql::test