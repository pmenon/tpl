#include "sql/operations/like_operators.h"
#include "util/test_harness.h"

namespace tpl::sql {

class LikeOperatorsTests : public TplTest {};

TEST_F(LikeOperatorsTests, ShortString) {
  EXPECT_FALSE(Like::Apply("abc", "a"));
  EXPECT_FALSE(Like::Apply("abc", "ab"));
  EXPECT_FALSE(Like::Apply("abc", "axc"));
  EXPECT_TRUE(Like::Apply("abc", "abc"));
}

TEST_F(LikeOperatorsTests, SingleCharacter_Wildcard) {
  // Character after single-character wildcard doesn't match
  EXPECT_FALSE(Like::Apply("forbes \\avenue", "forb_ \\\\venue"));
  // Now it does
  EXPECT_TRUE(Like::Apply("forbes \\avenue", "%b_s \\\\avenue"));

  // N '_'s must match N characters
  EXPECT_FALSE(Like::Apply("P Money", "__money"));
  EXPECT_TRUE(Like::Apply("P Money", "__Money"));
  EXPECT_TRUE(Like::Apply("P Money", "__M___y"));
}

TEST_F(LikeOperatorsTests, MultiCharacter_Wildcard) {
  // Must consume all '%'
  EXPECT_TRUE(Like::Apply("Money In The Bank", "_%%%%%%_"));
  EXPECT_TRUE(Like::Apply("Money In The Bank", "_%%%%%%%%%"));

  // Consume all '%', but also match last non-wildcard character
  EXPECT_FALSE(Like::Apply("Money In The Bank", "_%%%%%%%%x"));
  EXPECT_TRUE(Like::Apply("Money In The Bank", "_%%%%%%%%k"));

  // Pattern ending in escape
  EXPECT_TRUE(Like::Apply("Money In The Bank\\", "_%%%%%%%%k\\\\"));
}

}  // namespace tpl::sql
