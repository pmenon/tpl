#include "util/file.h"
#include "util/test_harness.h"

namespace tpl::util {

class FileTest : public TplTest {};

TEST_F(FileTest, CreateReadWrite) {
  auto f = File();
  EXPECT_NO_THROW(f.CreateTemp());

  auto s = std::string("Simple Test");
  EXPECT_NO_THROW(f.WriteFull(reinterpret_cast<byte *>(s.data()), s.length()));
  EXPECT_TRUE(f.Flush());

  char r[100];
  EXPECT_NO_THROW(f.ReadFullFromPosition(0, reinterpret_cast<byte *>(r), s.length()));

  EXPECT_EQ(std::string(r, s.length()), s);
}

}  // namespace tpl::util
