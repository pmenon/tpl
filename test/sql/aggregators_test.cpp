#include <sql/value.h>
#include "tpl_test.h"

#include "sql/aggregators.h"

namespace tpl::sql::test {

class AggregatorsTest : public TplTest {};

TEST_F(AggregatorsTest, CountTest) {
  //
  // Count on empty input
  //

  {
    CountAggregate count;
    EXPECT_EQ(0, count.GetCountResult().val);
  }

  //
  // Count on mixed NULL and non-NULL input
  //

  {
    // Even inputs are NULL
    CountAggregate count;
    for (u32 i = 0; i < 10; i++) {
      Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
      count.Advance(&val);
    }
    EXPECT_EQ(5, count.GetCountResult().val);
  }
}

TEST_F(AggregatorsTest, CountMergeTest) {
  // Even inputs are NULL
  CountAggregate count_1, count_2;

  // Insert into count_1
  for (u32 i = 0; i < 100; i++) {
    Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
    count_1.Advance(&val);
  }
  for (u32 i = 0; i < 100; i++) {
    Integer val(i);
    count_2.Advance(&val);
  }

  auto merged = count_1.GetCountResult().val + count_2.GetCountResult().val;

  count_1.Merge(count_2);

  EXPECT_EQ(merged, count_1.GetCountResult().val);
}

TEST_F(AggregatorsTest, SumIntegerTest) {
  //
  // SUM on empty input is null
  //

  {
    IntegerSumAggregate sum;
    EXPECT_TRUE(sum.GetResultSum().is_null);
  }

  //
  // Sum on mixed NULL and non-NULL input
  //

  {
    // [1, 3, 5, 7, 9]
    IntegerSumAggregate sum;
    for (u32 i = 0; i < 10; i++) {
      Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
      sum.AdvanceNullable(&val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null);
    EXPECT_EQ(25, sum.GetResultSum().val);
  }

  //
  // Sum on non-NULL input
  //

  {
    // [0..9]
    IntegerSumAggregate sum;
    for (u32 i = 0; i < 10; i++) {
      Integer val(i);
      sum.Advance(&val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null);
    EXPECT_EQ(45, sum.GetResultSum().val);
  }
}

}  // namespace tpl::sql::test
