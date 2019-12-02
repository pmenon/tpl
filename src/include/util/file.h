#pragma once

#include <filesystem>

#include "common/common.h"
#include "common/macros.h"

namespace tpl::util {

/**
 * Handle to a file.
 */
class File {
 public:
  /**
   * Various access methods.
   */
  enum class AccessMode : uint8_t { Create, ReadOnly, WriteOnly, ReadWrite };

  /**
   * Create a file handle to no particular file.
   */
  File() : fd_(kInvalid) {}

  /**
   * File handles cannot be copied.
   */
  DISALLOW_COPY(File);

  /**
   * Destructor.
   */
  ~File() { Close(); }

  /**
   * Move construct a new file from an existing file. The existing file is closed and will take
   * ownership of the new file.
   * @param other The file to move.
   */
  File(File &&other) noexcept : fd_(kInvalid) { std::swap(fd_, other.fd_); }

  /**
   * Move construct a new file from an existing file. The existing file is closed and will take
   * ownership of the new file.
   * @param other The file to move.
   */
  File &operator=(File &&other) noexcept {
    // First, close this file
    Close();

    // Swap descriptors
    std::swap(fd_, other.fd_);

    // Done
    return *this;
  }

  /**
   * Open the file at the given path and with the provided access mode.
   * @param path The path to the file.
   * @param access_mode The access mode.
   */
  void Open(const std::filesystem::path &path, AccessMode access_mode);

  /**
   * Create a file at the given path. If a file exists already, an exception is thrown.
   * @param path Path.
   */
  void Create(const std::filesystem::path &path);

  /**
   * Create a temporary file.
   */
  void CreateTemp();

  /**
   * Make a best-effort attempt to read @em len bytes of data from the current file position into
   * the provided output byte array @em data.
   * @param[out] data Buffer where file contents are written to. Must be large enough to store at
   *                  least @em len bytes of data.
   * @param len The maximum number of bytes to read.
   * @return The number of bytes read if >= 0, or the error code if < 0.
   */
  int32_t ReadFull(byte *data, std::size_t len) const;

  /**
   * Make a best-effort attempt to read @em len bytes of data from a specific position in the file
   * into the provided output byte array @em data.
   * @param offset The offset in the file to read from.
   * @param[out] data Buffer where file contents are written to. Must be large enough to store at
   *                  least @em len bytes of data.
   * @param len The maximum number of bytes to read.
   * @return The number of bytes read if >= 0, or the error code if < 0.
   */
  int32_t ReadFullFromPosition(std::size_t offset, byte *data, std::size_t len) const;

  /**
   * Attempt to read @em len bytes of data from the current file position into the provided output
   * buffer @em data. No effort is made to guarantee that all requested bytes are read, and no retry
   * logic is used to catch spurious failures.
   * @param[out] data Buffer where file contents are written to. Must be large enough to store at
   *                  least @em len bytes of data.
   * @param len The maximum number of bytes to read.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t Read(byte *data, std::size_t len) const;

  /**
   * Make a best-effort attempt to write @em len bytes from @em data into the file.
   * @param data The data to write.
   * @param len The number of bytes to write.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t WriteFull(const byte *data, std::size_t len) const;

  /**
   * Make a best-effort attempt to write @em len bytes from @em data into the file at the specified
   * byte offset.
   * @param offset Offset into the file to write.
   * @param data The data to write.
   * @param len The number of bytes to write.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t WriteFullAtPosition(std::size_t offset, byte *data, std::size_t len) const;

  /**
   * Attempt to write @em len bytes from @em data into the file. No effort is made to write all
   * requested bytes, and no retry logic is used to catch spurious failures.
   * @param data The data to write.
   * @param len The number of bytes to write.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t Write(const byte *data, std::size_t len) const;

  /**
   * Flush any in-memory buffered contents into the file.
   * @return True if the flush was successful; false otherwise.
   */
  bool Flush() const;

  /**
   * @return The size of the file, if valid and open, in bytes.
   */
  std::size_t Size() const;

  /**
   * @return True if the file is open; false otherwise.
   */
  bool IsOpen() const { return fd_ != kInvalid; }

  /**
   * Close the file. Does nothing if already closed, or not open.
   */
  void Close();

 private:
  // The file descriptor
  int32_t fd_;

  static constexpr int kInvalid = -1;
};

}  // namespace tpl::util