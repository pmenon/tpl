#include "util/file.h"
#include "util/sfc_gen.h"
#include "util/test_harness.h"

namespace tpl::util {

class FileTest : public TplTest {};

TEST_F(FileTest, CreateTemporaryFile) {
  // Create a temporary file and write a string into it
  auto f = File();
  f.CreateTemp(true);

  ASSERT_FALSE(f.HasError());
  ASSERT_TRUE(f.IsCreated());

  auto s = std::string("Simple Test");
  auto written = f.WriteFull(reinterpret_cast<byte *>(s.data()), s.length());
  ASSERT_FALSE(f.HasError());
  ASSERT_EQ(written, s.length());
  ASSERT_TRUE(f.Flush());
  ASSERT_EQ(s.size(), f.Length());

  char r[100];
  auto chars_read = f.ReadFullFromPosition(0, reinterpret_cast<byte *>(r), s.length());
  ASSERT_FALSE(f.HasError());
  ASSERT_EQ(chars_read, s.length());

  ASSERT_EQ(std::string(r, s.length()), s);
}

TEST_F(FileTest, Create) {
  auto path = std::filesystem::path("/tmp/tpl.TEMP." + std::to_string(SFC32{}()));

  {
    // Empty file
    File file;
    ASSERT_FALSE(file.IsOpen());
    ASSERT_EQ(File::Error::FAILED, file.GetErrorIndicator());
  }

  {
    // Open a file that doesn't exist
    File file(path, File::FLAG_OPEN | File::FLAG_READ);
    ASSERT_FALSE(file.IsOpen());
    ASSERT_EQ(File::Error::NOT_FOUND, file.GetErrorIndicator());
  }

  {
    // Open or create a file
    File file(path, File::FLAG_OPEN_ALWAYS | File::FLAG_READ);
    ASSERT_TRUE(file.IsOpen());
    ASSERT_TRUE(file.IsCreated());
    ASSERT_EQ(File::Error::OK, file.GetErrorIndicator());
  }

  {
    // Create a file that already exists
    File file(path, File::FLAG_CREATE | File::FLAG_READ);
    ASSERT_FALSE(file.IsOpen());
    ASSERT_FALSE(file.IsCreated());
    ASSERT_EQ(File::Error::EXISTS, file.GetErrorIndicator());
  }

  {
    // Open an existing file and check that closing works
    File file(path, File::FLAG_OPEN | File::FLAG_READ);
    ASSERT_TRUE(file.IsOpen());
    ASSERT_FALSE(file.IsCreated());
    file.Close();
    ASSERT_FALSE(file.IsOpen());
  }

  {
    // Overwrite an existing file
    File file(path, File::FLAG_CREATE_ALWAYS | File::FLAG_WRITE);
    ASSERT_TRUE(file.IsOpen());
    ASSERT_TRUE(file.IsCreated());
    ASSERT_EQ(File::Error::OK, file.GetErrorIndicator());
  }

  {
    // Create a temp file that's delete upon closing
    File file(path, File::FLAG_OPEN | File::FLAG_READ | File::FLAG_DELETE_ON_CLOSE);
    ASSERT_TRUE(file.IsOpen());
    ASSERT_FALSE(file.IsCreated());
    file.Close();
    ASSERT_FALSE(std::filesystem::exists(path));
  }
}

TEST_F(FileTest, ReadAndWrite) {
  auto path = std::filesystem::path("/tmp/tpl.TEMP." + std::to_string(SFC32{}()));

  File file(path, File::FLAG_OPEN_ALWAYS | File::FLAG_READ | File::FLAG_WRITE |
                      File::FLAG_DELETE_ON_CLOSE);
  ASSERT_TRUE(file.IsOpen());
  ASSERT_FALSE(file.HasError());

  // Write
  const std::string text =
      "Cash rules everything around me, C.R.E.A.M. get the money, dolla' dolla' bill ya'll";
  auto written = file.WriteFull(reinterpret_cast<const byte *>(text.data()), text.length());
  EXPECT_EQ(written, text.length());
  EXPECT_TRUE(file.Flush());
  EXPECT_EQ(text.size(), file.Length());

  // Read it back in
  char text_back[100];
  auto chars_read =
      file.ReadFullFromPosition(0, reinterpret_cast<byte *>(text_back), text.length());
  EXPECT_EQ(chars_read, text.length());
  EXPECT_EQ(std::string(text_back, text_back + chars_read), text);
}

TEST_F(FileTest, Write) {
  auto path = std::filesystem::path("/tmp/tpl.TEMP." + std::to_string(SFC32{}()));

  File file(path, File::FLAG_OPEN_ALWAYS | File::FLAG_READ | File::FLAG_WRITE |
                      File::FLAG_DELETE_ON_CLOSE);
  ASSERT_TRUE(file.IsOpen());
  ASSERT_FALSE(file.HasError());

  // Write something
  std::string text = "Test";
  auto written = file.WriteFull(reinterpret_cast<const byte *>(text.data()), text.length());
  EXPECT_EQ(written, text.length());
  EXPECT_TRUE(file.Flush());

  // Overwrite
  text = "A new string";
  written = file.WriteFullAtPosition(0, reinterpret_cast<const byte *>(text.data()), text.length());
  EXPECT_EQ(written, text.length());
  EXPECT_TRUE(file.Flush());

  // Read second string back in
  char text_back[100];
  auto chars_read =
      file.ReadFullFromPosition(0, reinterpret_cast<byte *>(text_back), text.length());
  EXPECT_EQ(chars_read, text.length());
  EXPECT_EQ(std::string(text_back, text_back + chars_read), text);
}

TEST_F(FileTest, Seek) {
  auto path = std::filesystem::path("/tmp/tpl.TEMP." + std::to_string(SFC32{}()));

  File file(path, File::FLAG_OPEN_ALWAYS | File::FLAG_READ | File::FLAG_WRITE |
                      File::FLAG_DELETE_ON_CLOSE);
  ASSERT_TRUE(file.IsOpen());
  ASSERT_FALSE(file.HasError());

  // Write something
  std::string text = "Test";
  auto written = file.WriteFull(reinterpret_cast<const byte *>(text.data()), text.length());
  EXPECT_EQ(written, text.length());
  EXPECT_TRUE(file.Flush());

  // Seek back two characters
  EXPECT_EQ(2, file.Seek(File::Whence::FROM_CURRENT, -2));

  // Try to read in 100 characters, but should only read in last two
  char text_back[100];
  auto chars_read = file.ReadFull(reinterpret_cast<byte *>(text_back), 100);
  EXPECT_EQ(chars_read, 2);
  EXPECT_EQ(std::string(text_back, text_back + chars_read), "st");
}

}  // namespace tpl::util
