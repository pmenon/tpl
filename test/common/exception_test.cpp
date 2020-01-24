#include "common/exception.h"

#include "util/test_harness.h"

namespace tpl {

class ExceptionTest : public TplTest {};

TEST_F(ExceptionTest, Simple) {
  EXPECT_THROW(throw NotImplementedException("Test"), NotImplementedException);

  std::cout << NotImplementedException("Test");
}

}  // namespace tpl
