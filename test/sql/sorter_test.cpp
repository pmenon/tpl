#include <sql/value.h>
#include "tpl_test.h"

#include "sql/sorter.h"

namespace tpl::sql::test {

class SorterTest : public TplTest {};


int test_compare(const byte *lhs, const byte *rhs)
{
  if(lhs == nullptr){
    return -1;
  }
  if(rhs == nullptr){
    return 1;
  }
  if(*reinterpret_cast<const int*>(lhs) < *reinterpret_cast<const int*>(rhs)) {
    return -1;
  }
  if(*reinterpret_cast<const int*>(lhs) == *reinterpret_cast<const int*>(rhs)) {
    return 0;
  }
  return 1;
}

TEST_F(SorterTest, Test) {
  //
  // Count on empty input
  //

  {
    util::Region region("test");
    Sorter sorter(&region, test_compare, sizeof(int));
    int *alloced = reinterpret_cast<int*>(sorter.AllocInputTuple());
    *alloced = 5;
    alloced = reinterpret_cast<int*>(sorter.AllocInputTuple());
    *alloced = 24;
    alloced = reinterpret_cast<int*>(sorter.AllocInputTuple());
    *alloced = 0;
    sorter.Sort();
    SorterIterator iterator(&sorter);
    EXPECT_EQ(*reinterpret_cast<const int*>(iterator.operator*()), 0);
  }

}

}  // namespace tpl::sql::test

