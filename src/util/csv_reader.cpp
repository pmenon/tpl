#include "util/csv_reader.h"

#include <immintrin.h>
#include <cstring>

namespace tpl::util {

CSVReader::CSVReader(std::string file_path, uint32_t num_cols, char delimiter, char quote,
                     char escape)
    : file_path_(std::move(file_path)),
      buf_(nullptr),
      buf_pos_(nullptr),
      buf_end_(nullptr),
      buf_alloc_size_(kDefaultBufferSize),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape) {
  row_.cells.resize(num_cols);
  for (CSVCell &cell : row_.cells) {
    cell.ptr = nullptr;
    cell.len = 0;
  }
}

CSVReader::CSVReader(const char *file_path, const uint32_t num_cols, const char delimiter,
                     const char quote, const char escape)
    : CSVReader(std::string(file_path), num_cols, delimiter, quote, escape) {}

bool CSVReader::FillBuffer() {
  // Copy left-over data to the start of the buffer.
  const uint64_t remaining = buf_end_ - buf_pos_;

  if (remaining > 0) {
    std::memcpy(buf_.get(), buf_pos_, remaining);
  }

  buf_pos_ = buf_.get();
  buf_end_ = buf_pos_ + remaining;

  // If the buffer is too small we'll allocate a new one double the size
  if (remaining == buf_alloc_size_) {
    buf_alloc_size_ = std::min(buf_alloc_size_ * 2, kMaxAllocSize);
    auto new_buf = std::unique_ptr<char[]>(new char[buf_alloc_size_ + kExtraPadding]);
    std::memcpy(new_buf.get(), buf_.get(), remaining);
    buf_ = std::move(new_buf);
    stats_.num_reallocs++;
  }

  // Try to read a full buffer's worth of data from the input file.
  const auto available = &buf_[buf_alloc_size_] - buf_end_;
  const auto actual_bytes_read = file_.ReadFull(reinterpret_cast<byte *>(buf_end_), available);
  if (actual_bytes_read <= 0) {
    return false;
  }

  // Bump up the end
  buf_end_ += actual_bytes_read;

  // Collect stats
  stats_.num_reads++;
  stats_.bytes_read += static_cast<uint32_t>(actual_bytes_read);

  // Done
  return true;
}

bool CSVReader::Initialize() {
  // Let's try to open the file
  file_.Open(file_path_, util::File::FLAG_OPEN | util::File::FLAG_READ);

  if (!file_.IsOpen()) {
    return false;
  }

  // Allocate buffer space, and setup internal buffer pointers
  buf_ = std::make_unique<char[]>(buf_alloc_size_ + kExtraPadding);
  buf_pos_ = buf_end_ = buf_.get();

  // Fill read-buffer
  return FillBuffer();
}

// Try parse will try to parse one line of input. If successful, it will return
// ParseOk. If it needs more data, it returns
CSVReader::ParseResult CSVReader::TryParse() {
  const auto check_quoted = [&](const char *buf) {
    const auto special = _mm_setr_epi8(quote_, escape_, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    const auto data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(buf));
    return _mm_cmpistri(special, data, _SIDD_CMP_EQUAL_ANY);
  };

  const auto check_unquoted = [&](const char *buf) {
    const auto special =
        _mm_setr_epi8(delimiter_, escape_, '\r', '\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    const auto data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(buf));
    return _mm_cmpistri(special, data, _SIDD_CMP_EQUAL_ANY);
  };

  int32_t ret;

  // The current cell
  CSVCell *cell = &row_.cells[0];

  // The running pointer into the buffer
  char *ptr = buf_pos_;

#define RETURN_IF_AT_END()            \
  if (ptr >= buf_end_) {              \
    return ParseResult::NeedMoreData; \
  }

#define FINISH_CELL()     \
  cell->ptr = cell_start; \
  cell->len = ptr - cell_start;

#define FINISH_QUOTED_CELL() \
  cell->ptr = cell_start;    \
  cell->len = ptr - cell_start - 1;

cell_start:
  char *cell_start = ptr;

  // The first check we do is if we've reached the end of a line. This can be
  // caused by an empty last cell.

  RETURN_IF_AT_END();
  if (*ptr == '\r' || *ptr == '\n') {
    buf_pos_ = ptr + 1;
    stats_.num_lines++;
    return ParseResult::Ok;
  }

  // If the first character in a cell is the quote character, it's deemed a
  // "quoted cell". Quoted cells can contain more quote characters, escape
  // characters, new lines, or carriage returns - all of which have to be
  // escaped, of course. However, quoted fields must always end in a quoting
  // character followed by a delimiter character.

  if (*ptr == quote_) {
    cell_start = ++ptr;
  quoted_cell:
    while (true) {
      RETURN_IF_AT_END();
      ret = check_quoted(ptr);
      if (ret != 16) {
        ptr += ret + 1;
        break;
      }
      ptr += 16;
    }

    RETURN_IF_AT_END();
    if (*ptr == delimiter_) {
      FINISH_QUOTED_CELL();
      cell++;
      ptr++;
      goto cell_start;
    }
    if (*ptr == '\r' || *ptr == '\n') {
      FINISH_QUOTED_CELL();
      buf_pos_ = ptr + 1;
      stats_.num_lines++;
      return ParseResult::Ok;
    }
    ptr++;
    goto quoted_cell;
  }

  // The first character isn't a quote, so this is a vanilla unquoted field. We
  // just need to find the next delimiter. However, these fields can also have
  // escape characters which we need to handle.

unquoted_cell:
  while (true) {
    RETURN_IF_AT_END();
    ret = check_unquoted(ptr);
    if (ret != 16) {
      ptr += ret;
      break;
    }
    ptr += 16;
  }

  RETURN_IF_AT_END();
  if (*ptr == delimiter_) {
    FINISH_CELL();
    cell++;
    ptr++;
    goto cell_start;
  }
  if (*ptr == '\r' || *ptr == '\n') {
    FINISH_CELL();
    buf_pos_ = ptr + 1;
    stats_.num_lines++;
    return ParseResult::Ok;
  }
  ptr++;
  goto unquoted_cell;
}

bool CSVReader::Advance() {
  do {
    switch (TryParse()) {
      case ParseResult::Ok:
        return true;
      case ParseResult::NeedMoreData:
        break;
    }
  } while (FillBuffer());

  // There's no more data in the buffer.
  return false;
}

}  // namespace tpl::util
