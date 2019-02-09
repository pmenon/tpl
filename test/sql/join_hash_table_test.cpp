#include "gtest/gtest.h"

#include <random>

#include "tpl_test.h"

#include "sql/runtime/join_hash_table.h"
#include "util/hash.h"

namespace tpl::sql::test {

class JoinHashTableTest : public TplTest {
 public:
  JoinHashTableTest() : region_(GetTestName()), join_hash_table_(region()) {}

  util::Region *region() { return &region_; }

  runtime::JoinHashTable *join_hash_table() { return &join_hash_table_; }

  runtime::GenericHashTable *inner_hash_table() {
    return join_hash_table()->hash_table();
  }

 private:
  util::Region region_;

  runtime::JoinHashTable join_hash_table_;
};

struct Tuple {
  u64 a, b, c, d;
};

TEST_F(JoinHashTableTest, LazyInsertionTest) {
  // Test data
  u32 num_tuples = 10;
  std::vector<Tuple> tuples(num_tuples);

  // Populate test data
  {
    std::mt19937 generator;
    std::uniform_int_distribution<u64> distribution;

    for (u32 i = 0; i < num_tuples; i++) {
      tuples[i].a = distribution(generator);
      tuples[i].b = distribution(generator);
      tuples[i].c = distribution(generator);
      tuples[i].d = distribution(generator);
    }
  }

  // The table
  for (const auto &tuple : tuples) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const u8 *>(&tuple.a),
                                       sizeof(tuple.a));
    auto *e = reinterpret_cast<Tuple *>(
        join_hash_table()->AllocInputTuple(hash_val, sizeof(Tuple)));
    *e = tuple;
  }

  EXPECT_EQ(num_tuples, join_hash_table()->num_elems());
  EXPECT_EQ(0u, inner_hash_table()->num_elements());

  // Try to build

  join_hash_table()->Build();

  EXPECT_EQ(num_tuples, join_hash_table()->num_elems());
  EXPECT_EQ(num_tuples, inner_hash_table()->num_elements());
}

}  // namespace tpl::sql::test