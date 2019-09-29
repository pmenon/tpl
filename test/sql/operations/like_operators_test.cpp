#include <string>

#include "sql/operations/like_operators.h"
#include "util/test_harness.h"

namespace tpl::sql {

class LikeOperatorsTests : public TplTest {};

TEST_F(LikeOperatorsTests, ShortString) {
  // 'abc' LIKE 'a' = false
  std::string s = "abc";
  std::string p = "a";
  EXPECT_FALSE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // 'abc' LIKE 'ab' = false
  s = "abc";
  p = "ab";
  EXPECT_FALSE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // 'abc' LIKE 'axc' = false
  s = "abc";
  p = "axc";
  EXPECT_FALSE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // 'abc' LIKE 'abc' = true
  s = "abc";
  p = "abc";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));
}

TEST_F(LikeOperatorsTests, SingleCharacter_Wildcard) {
  // Character after single-character wildcard doesn't match
  std::string s = "forbes \\avenue";
  std::string p = "forb_ \\\\venue";
  EXPECT_FALSE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // Now it does
  p = "%b_s \\\\avenue";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // N '_'s must match N characters

  // mismatched 'M'
  s = "P Money";
  p = "__money";
  EXPECT_FALSE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // Match
  p = "__Money";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // Match
  p = "__M___y";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));
}

TEST_F(LikeOperatorsTests, MultiCharacter_Wildcard) {
  // Must consume all '%'
  std::string s = "Money In The Bank";
  std::string p = "_%%%%%%_";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  p = "_%%%%%%%%%";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // Consume all '%', but also match last non-wildcard character
  p = "_%%%%%%%%x";
  EXPECT_FALSE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  p = "_%%%%%%%%k";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));

  // Pattern ending in escape
  s = "Money In The Bank\\";
  p = "_%%%%%%%%k\\\\";
  EXPECT_TRUE(Like::Apply(s.c_str(), s.length(), p.c_str(), p.length()));
}

}  // namespace tpl::sql
