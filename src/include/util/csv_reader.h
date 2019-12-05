#pragma once

#include <string>
#include <vector>

#include "common/common.h"
#include "util/file.h"

namespace tpl::util {

/**
 * A fast CSV file reader.
 */
class CSVReader {
 public:
  // 64K buffer size
  static constexpr uint32_t kDefaultBufferSize = 64 * KB;

  // We allocate a maximum of 1GB for the line buffer
  static constexpr uint64_t kMaxAllocSize = 1 * GB;

  // Extra padding added to the buffer to ensure we can always read at least
  static constexpr uint32_t kExtraPadding = 32;

  /**
   * A cell in a row in the CSV.
   */
  struct CSVCell {
    // Pointer to the cell's data
    const char *ptr;
    // Length of the data in bytes
    std::size_t len;

    /**
     * @return This cell's value converted into a 64-bit signed integer.
     */
    int64_t AsInteger() const { return std::strtol(ptr, nullptr, 10); }

    /**
     * @return This cell's value convert into a 64-bit flaoting point value.
     */
    double AsDouble() const { return std::strtod(ptr, nullptr); }

    /**
     * @return This cell's value as a string.
     */
    std::string_view AsString() const { return std::string_view(ptr, len); }
  };

  /**
   * A row in the CSV.
   */
  struct CSVRow {
    // The cells/attributes in this row.
    std::vector<CSVCell> cells;
  };

  /**
   * This structure tracks various statistics while we scan the CSV
   */
  struct Stats {
    // The number of times we had to re-allocate buffer due to long lines.
    uint32_t num_reallocs = 0;
    // The number of times we called Read() from the file.
    uint32_t num_reads = 0;
    // The total bytes read.
    uint32_t bytes_read = 0;
    // The number of lines in the CSV
    uint32_t num_lines = 0;
  };

  /**
   * Constructor.
   * @param file_path The full path to the CSV file.
   * @param num_cols The number of columns to expect.
   * @param delimiter The character that separates columns within a row.
   * @param quote The quoting character used to quote data (i.e., strings).
   * @param escape The character appearing before the quote character.
   */
  CSVReader(const char *file_path, uint32_t num_cols, char delimiter = ',', char quote = '"',
            char escape = '"');

  /**
   * Constructor.
   * @param file_path The full path to the CSV file.
   * @param num_cols The number of columns to expect.
   * @param delimiter The character that separates columns within a row.
   * @param quote The quoting character used to quote data (i.e., strings).
   * @param escape The character appearing before the quote character.
   */
  CSVReader(std::string file_path, uint32_t num_cols, char delimiter = ',', char quote = '"',
            char escape = '"');

  /**
   * Initialize the scan.
   * @return True if success; false otherwise.
   */
  bool Initialize();

  /**
   * Advance to the next row.
   * @return True if there is another row; false otherwise.
   */
  bool Advance();

  /**
   * @return Return the current row.
   */
  const CSVRow *GetRow() const noexcept { return &row_; }

  /**
   * @return Return statistics collected during parsing.
   */
  const Stats *GetStatistics() const noexcept { return &stats_; }

 private:
  // Read a buffer's worth of data from the CSV file. Any left-over data in the
  // buffer is copied to the from, and new file content is written afterwards.
  bool FillBuffer();

  // The result of an attempted parse
  enum class ParseResult { Ok, NeedMoreData };

  // Read the next line from the CSV file
  ParseResult TryParse();

 private:
  // The path to the CSV file.
  const std::string file_path_;

  // The CSV file handle.
  util::File file_;

  // The read-buffer where raw file contents are read into.
  std::unique_ptr<char[]> buf_;
  // Position in the buffer where the next row begins
  char *buf_pos_;
  // Number of active bytes in the buffer. Is less than or equal to the
  // allocation size.
  char *buf_end_;
  // The allocation size to use for buffers
  std::size_t buf_alloc_size_;

  // The active current row
  CSVRow row_;

  // The column delimiter, quote, and escape characters configured for this CSV
  char delimiter_;
  char quote_;
  char escape_;

  // Various statistics
  Stats stats_;
};

}  // namespace tpl::util
