#include "tpl_test.h"

#include <numeric>
#include <random>
#include <unordered_set>

#include "vm/constants_array_builder.h"

namespace tpl::vm::test {

class ConstantArrayBuilder : public TplTest {};

TEST_F(ConstantArrayBuilder, IntsTest) {
  /*
   * In this first sub-test, check unique integers on a small scale
   */
  {
    ConstantsArrayBuilder builder;

    uint32_t one = 1;
    uint32_t two = 2;

    auto first = builder.Insert(one);
    auto second = builder.Insert(two);
    auto third = builder.Insert(one);

    EXPECT_NE(first, second);
    EXPECT_EQ(first, third);
  }

  /*
   * Insert unique integers in the range [0,1000). Shuffle the input vector and
   * try to reinsert them ensuring all return non-new indexes.
   */
  {
    uint32_t num_ints = 1000;
    std::vector<uint32_t> ints(num_ints);
    std::iota(ints.begin(), ints.end(), 0);

    std::unordered_set<uint32_t> indexes;

    // Fill the builder with unique integers
    ConstantsArrayBuilder builder;
    for (const auto i : ints) {
      auto idx = builder.Insert(i);
      EXPECT_EQ(0u, indexes.count(idx));
      indexes.insert(idx);
    }

    // Shuffle input
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(ints.begin(), ints.end(), g);

    for (const auto i : ints) {
      auto idx = builder.Insert(i);
      EXPECT_EQ(1u, indexes.count(idx));
    }
  }
}

TEST_F(ConstantArrayBuilder, StringsTest) {
  ConstantsArrayBuilder builder;

  const char *one = "111";
  const char *two = "222";

  auto first = builder.Insert(one);
  auto second = builder.Insert(two);

  EXPECT_NE(first, second);

  auto third = builder.Insert(one);

  EXPECT_EQ(first, third);

  auto fourth = builder.Insert(two);

  EXPECT_EQ(second, fourth);
}

TEST_F(ConstantArrayBuilder, MixedTypeTest) {
  ConstantsArrayBuilder builder;

  const char *one = "111";
  const uint32_t two = 222;
  const char *three = "333";
  const uint32_t four = 44;

  auto first = builder.Insert(one);
  auto second = builder.Insert(two);
  auto third = builder.Insert(three);
  auto fourth = builder.Insert(four);

  EXPECT_NE(first, second);
  EXPECT_NE(first, third);
  EXPECT_NE(first, fourth);
  EXPECT_NE(second, third);
  EXPECT_NE(second, fourth);
  EXPECT_NE(third, fourth);
}

}  // namespace tpl::vm::test