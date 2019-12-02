#include "util/file.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <system_error>

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"

namespace tpl::util {

#if defined(NDEBUG)

#define HANDLE_EINTR(x)                                     \
  ({                                                        \
    decltype(x) eintr_wrapper_result;                       \
    do {                                                    \
      eintr_wrapper_result = (x);                           \
    } while (eintr_wrapper_result == -1 && errno == EINTR); \
    eintr_wrapper_result;                                   \
  })

#else

#define HANDLE_EINTR(x)                                                                      \
  ({                                                                                         \
    uint32_t eintr_wrapper_counter = 0;                                                      \
    decltype(x) eintr_wrapper_result;                                                        \
    do {                                                                                     \
      eintr_wrapper_result = (x);                                                            \
    } while (eintr_wrapper_result == -1 && errno == EINTR && eintr_wrapper_counter++ < 100); \
    eintr_wrapper_result;                                                                    \
  })

#endif

void File::Open(const std::filesystem::path &path, File::AccessMode access_mode) {
  // Close the existing file if it's open
  Close();

  int flags;
  switch (access_mode) {
    case AccessMode::Create:
      flags = O_CREAT;
      break;
    case AccessMode::ReadOnly:
      flags = O_RDWR;
      break;
    case AccessMode::WriteOnly:
      flags = O_WRONLY;
      break;
    case AccessMode::ReadWrite:
      flags = O_RDWR;
      break;
  }

  // Open
  const int32_t fd = HANDLE_EINTR(open(path.c_str(), flags));

  // Check error
  if (fd == kInvalid) {
    throw Exception(ExceptionType::File, fmt::format("unable to read file '{}}': {}", path.string(),
                                                     std::strerror(errno)));
  }

  // Done
  fd_ = fd;
}

void File::Create(const std::filesystem::path &path) { Open(path, AccessMode::Create); }

void File::CreateTemp() {
  // Close the existing file if it's open
  Close();

  // Attempt to create a temporary file
  char tmp[] = "/tmp/tpl.XXXXXX";
  fd_ = mkstemp(tmp);

  // Fail?
  if (fd_ == kInvalid) {
    throw Exception(ExceptionType::File,
                    fmt::format("unable to create temporary file '{}'", std::strerror(errno)));
  }
}

int32_t File::ReadFull(byte *data, size_t len) const {
  // Ensure open
  TPL_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_read = 0;
  int32_t ret;
  do {
    ret = HANDLE_EINTR(read(fd_, data + bytes_read, len - bytes_read));
    if (ret <= 0) {
      break;
    }
    bytes_read += ret;
  } while (bytes_read < len);

  return bytes_read > 0 ? bytes_read : ret;
}

int32_t File::ReadFullFromPosition(std::size_t offset, byte *data, std::size_t len) const {
  // Ensure open
  TPL_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_read = 0;
  int32_t ret;
  do {
    ret = HANDLE_EINTR(pread(fd_, data + bytes_read, len - bytes_read, offset + bytes_read));
    if (ret <= 0) {
      break;
    }
    bytes_read += ret;
  } while (bytes_read < len);

  return bytes_read > 0 ? bytes_read : ret;
}

int32_t File::Read(byte *data, std::size_t len) const { return HANDLE_EINTR(read(fd_, data, len)); }

int32_t File::WriteFull(const byte *data, size_t len) const {
  // Ensure open
  TPL_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_written = 0;
  int32_t rv;
  do {
    rv = HANDLE_EINTR(write(fd_, data + bytes_written, len - bytes_written));
    if (rv <= 0) {
      break;
    }
    bytes_written += rv;
  } while (bytes_written < len);

  return bytes_written > 0 ? bytes_written : rv;
}

int32_t File::WriteFullAtPosition(std::size_t offset, byte *data, std::size_t len) const {
  // Ensure open
  TPL_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_written = 0;
  int32_t rv;
  do {
    rv = HANDLE_EINTR(
        pwrite(fd_, data + bytes_written, len - bytes_written, offset + bytes_written));
    if (rv <= 0) {
      break;
    }
    bytes_written += rv;
  } while (bytes_written < len);

  return bytes_written > 0 ? bytes_written : rv;
}

int32_t File::Write(const byte *data, std::size_t len) const {
  return HANDLE_EINTR(write(fd_, data, len));
}

bool File::Flush() const {
#if defined(OS_LINUX)
  return HANDLE_EINTR(fdatasync(fd_)) == 0;
#else
  return HANDLE_EINTR(fsync(fd_)) == 0;
#endif
}

std::size_t File::Size() const {
  // Ensure open
  TPL_ASSERT(IsOpen(), "File must be open before reading");

  // Save the current position
  off_t curr_off = lseek(fd_, 0, SEEK_CUR);
  if (curr_off == -1) {
    throw Exception(ExceptionType::File,
                    fmt::format("unable to read current position in file: {}", strerror(errno)));
  }

  // Seek to the end of the file, returning the new file position i.e., the
  // size of the file in bytes.
  off_t off = lseek(fd_, 0, SEEK_END);
  if (off == -1) {
    throw Exception(ExceptionType::File,
                    fmt::format("unable to move file position to end file: {}", strerror(errno)));
  }

  off_t restore = lseek(fd_, curr_off, SEEK_SET);
  if (restore == -1) {
    throw Exception(
        ExceptionType::File,
        fmt::format("unable to restore position after moving to the end: {}", strerror(errno)));
  }

  // Restore position
  return static_cast<std::size_t>(off);
}

void File::Close() {
  if (IsOpen()) {
    close(fd_);
    fd_ = kInvalid;
  }
}

}  // namespace tpl::util