#include "util/csv_reader.h"
#include "util/file.h"
#include "util/test_harness.h"

namespace tpl::util {

class CSVReaderTest : public TplTest {};

TEST_F(CSVReaderTest, CheckEscaping) {
  {
    CSVString str("10,\"BLAHBLAH\",\"Special \"\"AF\"\" string\",1000\n");
    CSVReader reader(&str);

    ASSERT_TRUE(reader.Initialize());
    ASSERT_TRUE(reader.Advance());
    auto row = reader.GetRow();
    EXPECT_EQ(4u, row->count);
    EXPECT_EQ(10, row->cells[0].AsInteger());
    EXPECT_EQ("BLAHBLAH", row->cells[1].AsString());
    EXPECT_EQ("Special \"AF\" string", row->cells[2].AsString());
    EXPECT_EQ(1000, row->cells[3].AsInteger());
    EXPECT_EQ(1u, reader.GetStatistics()->num_lines);
  }

  {
    CSVString str(
        "1,two,\"\nNewRow\n\"\n"
        "3,four,NormalRow\n");
    CSVReader reader(&str);
    ASSERT_TRUE(reader.Initialize());

    // First row
    EXPECT_TRUE(reader.Advance());
    EXPECT_EQ(3u, reader.GetRow()->count);
    EXPECT_EQ(1, reader.GetRow()->cells[0].AsInteger());
    EXPECT_EQ("two", reader.GetRow()->cells[1].AsString());
    EXPECT_EQ("\nNewRow\n", reader.GetRow()->cells[2].AsString());
  }
}

TEST_F(CSVReaderTest, EmptyCellsAndRows) {
  CSVString str(
      "1,two,three\n"
      ",,\n"
      "4,,six\n");
  CSVReader reader(&str);
  reader.Initialize();

  // First row
  EXPECT_TRUE(reader.Advance());
  EXPECT_EQ(3u, reader.GetRow()->count);
  EXPECT_EQ(1, reader.GetRow()->cells[0].AsInteger());
  EXPECT_EQ("two", reader.GetRow()->cells[1].AsString());
  EXPECT_EQ("three", reader.GetRow()->cells[2].AsString());

  // Second row is empty
  EXPECT_TRUE(reader.Advance());
  EXPECT_EQ(3u, reader.GetRow()->count);
  EXPECT_TRUE(reader.GetRow()->cells[0].IsEmpty());
  EXPECT_TRUE(reader.GetRow()->cells[1].IsEmpty());
  EXPECT_TRUE(reader.GetRow()->cells[2].IsEmpty());

  // Third row
  EXPECT_TRUE(reader.Advance());
  EXPECT_EQ(3u, reader.GetRow()->count);
  EXPECT_EQ(4, reader.GetRow()->cells[0].AsInteger());
  EXPECT_TRUE(reader.GetRow()->cells[1].IsEmpty());
  EXPECT_EQ("six", reader.GetRow()->cells[2].AsString());
}

TEST_F(CSVReaderTest, CheckUnquoted) {
  CSVString str(
      "1,PA,498960,30.102261,-81.711777,Residential,Masonry,1\n"
      "2,CA,132237,30.063936,-81.707664,Residential,Wood,3\n"
      "3,NY,190724,29.089579,-81.700455,Residential,Masonry,1\n"
      "4,FL,0,30.063236,-81.707703,Residential,Wood,3\n"
      "5,WA,5,30.060614,-81.702675,Residential,Masonry,1\n"
      "6,TX,0,30.063236,-81.707703,Residential,Wood,3\n"
      "7,MA,0,30.102226,-81.713882,Commercial,Masonry,1\n");
  CSVReader reader(&str);
  ASSERT_TRUE(reader.Initialize());

  for (uint32_t id = 1; reader.Advance(); id++) {
    auto row = reader.GetRow();
    EXPECT_EQ(8u, row->count);
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
