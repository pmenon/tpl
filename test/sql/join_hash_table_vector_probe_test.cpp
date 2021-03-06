#include <random>
#include <vector>

#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/hash_util.h"
#include "util/test_harness.h"

namespace tpl::sql {

struct BuildRow {
  uint64_t key, val1, val2, val3;
  // Constructor.
  explicit BuildRow(uint64_t key) : BuildRow(key, 0, 0, 0) {}
  // Constructor.
  BuildRow(uint64_t key, uint64_t val_1, uint64_t val_2, uint64_t val_3)
      : key(key), val1(val_1), val2(val_2), val3(val_3) {}
  // Hash.
  hash_t Hash() const { return util::HashUtil::HashMurmur(key); }
};

// Build a join hash table over the given input data.
void BuildJHT(JoinHashTable *table, const std::vector<BuildRow> &data) {
  for (const auto &row : data) {
    auto table_row = reinterpret_cast<BuildRow *>(table->AllocInputTuple(row.Hash()));
    *table_row = row;
  }
  table->Build();
}

class JoinHashTableVectorProbeTest : public TplTest {
 public:
  JoinHashTableVectorProbeTest() : memory_(nullptr) {}

  MemoryPool *Memory() { return &memory_; }

 private:
  MemoryPool memory_;
};

TEST_F(JoinHashTableVectorProbeTest, EmptyJoinProbe) {
  JoinHashTable table(Memory(), sizeof(BuildRow));
  BuildJHT(&table, {});

  // The input to the probe.
  VectorProjection input;
  input.Initialize({TypeId::BigInt});
  input.Reset(100);
  VectorOps::Generate(input.GetColumn(0), 1, 1);

  // Test: create an INNER-join probe. No tuples should find matches since the
  //       hash table is empty.
  {
    JoinHashTableVectorProbe probe(table, planner::LogicalJoinType::INNER, {0});
    probe.Init(&input);
    EXPECT_FALSE(probe.Next(&input)) << "Empty join table should not find matches for inner join";
  }

  // Test: create an ANTI-join probe. All tuples should find matches since the
  //       hash table is empty.
  {
    JoinHashTableVectorProbe probe(table, planner::LogicalJoinType::ANTI, {0});
    probe.Init(&input);
    EXPECT_TRUE(probe.Next(&input));
    EXPECT_EQ(input.GetTotalTupleCount(), probe.GetMatches()->GetCount());
  }

  // Test: create a SEMI-join probe. No tuples should find matches since the
  //       hash table is empty.
  {
    JoinHashTableVectorProbe probe(table, planner::LogicalJoinType::SEMI, {0});
    probe.Init(&input);
    EXPECT_FALSE(probe.Next(&input));
  }
}

TEST_F(JoinHashTableVectorProbeTest, SimpleJoinProbe) {
  const uint32_t num_44_dups = 10;

  // Insert rows whose keys are in the range [0,100] in increments of 2.
  std::vector<BuildRow> rows;
  for (uint32_t i = 0; i < 100; i += 2) rows.emplace_back(i);

  // Key 44 has 10 duplicates. There's already one from the previous insertion
  // so insert num_44_dups-1 now.
  for (uint32_t i = 0; i < num_44_dups - 1; i++) rows.emplace_back(44);

  // Build table.
  JoinHashTable table(Memory(), sizeof(BuildRow));
  BuildJHT(&table, rows);

  // The input to the probe.
  VectorProjection input;
  input.Initialize({TypeId::BigInt});
  input.Reset(100);
  VectorOps::Generate(input.GetColumn(0), 0, 1);

  // Test: INNER-join should find all matches.
  {
    uint32_t num_iters = 0;
    JoinHashTableVectorProbe probe(table, planner::LogicalJoinType::INNER, {0});
    for (probe.Init(&input); probe.Next(&input); num_iters++) {
      auto probe_keys = input.GetColumn(0);
      auto matches = probe.GetMatches();
      auto match_filter = matches->GetFilteredTupleIdList();
      input.SetFilteredSelections(*match_filter);

      // Check matching keys.
      match_filter->ForEach([&](uint64_t i) {
        auto probe_key = reinterpret_cast<uint64_t *>(probe_keys->GetData())[i];
        auto build_key =
            reinterpret_cast<HashTableEntry **>(matches->GetData())[i]->PayloadAs<BuildRow>()->key;
        EXPECT_EQ(probe_key, build_key);
      });
    }
    EXPECT_EQ(num_44_dups, num_iters);
  }
}

}  // namespace tpl::sql
