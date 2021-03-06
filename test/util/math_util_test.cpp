#include "util/math_util.h"
#include "util/test_harness.h"

namespace tpl::util {

TEST(MathUtilTest, AlignToTest) {
  EXPECT_EQ(2u, MathUtil::AlignTo(1, 2));
  EXPECT_EQ(4u, MathUtil::AlignTo(4, 4));
  EXPECT_EQ(8u, MathUtil::AlignTo(4, 8));
  EXPECT_EQ(8u, MathUtil::AlignTo(8, 8));
  EXPECT_EQ(12u, MathUtil::AlignTo(9, 4));
  EXPECT_EQ(16u, MathUtil::AlignTo(9, 8));
}

}  // namespace tpl::util
