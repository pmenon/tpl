#include "util/csv_reader.h"
#include "util/file.h"
#include "util/test_harness.h"

namespace tpl::util {

/**
 * For these tests, just create a new enumeration for the test file you want to test, and a
 * generator function that creates the string you want in the file. The rest will be taken care of
 * for you.
 *
 * If you want to use read the file in a test, use CSVReaderTest::GetTestFileName() to get the
 * physical name of the file.
 */
class CSVReaderTest : public TplTest {
 public:
  enum CsvFile {
    Escape,
    Random,
  };

  CSVReaderTest() {
    files_[Escape] = std::make_pair("./escape.csv", []() {
      return "10,\"BLAHBLAH\",\"This string is \"\"special\"\"\",1000\n";
    });

    files_[Random] = std::make_pair("./random.csv", [] {
      // clang-format off
      return "1,PA,498960,30.102261,-81.711777,Residential,Masonry,1\n"
             "2,CA,132237,30.063936,-81.707664,Residential,Wood,3\n"
             "3,NY,190724,29.089579,-81.700455,Residential,Masonry,1\n"
             "4,FL,0,30.063236,-81.707703,Residential,Wood,3\n"
             "5,WA,5,30.060614,-81.702675,Residential,Masonry,1\n"
             "6,TX,0,30.063236,-81.707703,Residential,Wood,3\n"
             "7,MA,0,30.102226,-81.713882,Commercial,Masonry,1\n";
      // clang-format on
    });

    InitTestFiles();
  }

  ~CSVReaderTest() override { CleanupTestFiles(); }

  std::string GetTestFileName(CsvFile type) { return files_[type].first; }

 private:
  void InitTestFiles() {
    for (const auto &[_, p] : files_) {
      auto &[name, gen] = p;

      // Delete file if already exists, then create it with some dummy data.
      std::filesystem::remove(name);

      // Create new
      File file(name, File::FLAG_OPEN_ALWAYS | File::FLAG_WRITE);
      const std::string data = gen();
      file.WriteFullAtPosition(0, reinterpret_cast<const byte *>(data.data()), data.length());
      file.Flush();
    }
  }

  void CleanupTestFiles() {
    for (const auto &[_, p] : files_) {
      std::filesystem::remove(p.first);
    }
  }

 private:
  std::unordered_map<CsvFile, std::pair<std::string, std::function<std::string()>>> files_;
};

TEST_F(CSVReaderTest, CheckEscaping) {
  CSVReader reader(GetTestFileName(Escape), 4);
  ASSERT_TRUE(reader.Initialize());
  ASSERT_TRUE(reader.Advance());
  auto row = reader.GetRow();
  EXPECT_EQ(10, row->cells[0].AsInteger());
  EXPECT_EQ("BLAHBLAH", row->cells[1].AsString());
  EXPECT_EQ(1000, row->cells[3].AsInteger());
  EXPECT_EQ(1u, reader.GetStatistics()->num_lines);
}

TEST_F(CSVReaderTest, CheckUnquoted) {
  CSVReader reader(GetTestFileName(Random), 8);
  ASSERT_TRUE(reader.Initialize());

  for (uint32_t id = 1; reader.Advance(); id++) {
    auto row = reader.GetRow();
    // First column is increasing ID
    EXPECT_EQ(id, row->cells[0].AsInteger());
    // Fourth column is always negative double
    EXPECT_LT(row->cells[4].AsDouble(), 0);
    EXPECT_LT(row->cells[4].AsDouble(), -80);
    EXPECT_GT(row->cells[4].AsDouble(), -82);
    // All rows are 'Residential' except the last, which is 'Commercial'
    EXPECT_EQ(id < 7 ? "Residential" : "Commercial", row->cells[5].AsString());
    // Odd numbered rows are built with 'Masonry', the others are 'Wood'
    EXPECT_EQ(id % 2 == 0 ? "Wood" : "Masonry", row->cells[6].AsString());
  }
}

}  // namespace tpl::util
