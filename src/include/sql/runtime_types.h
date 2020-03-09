#pragma once

#include <algorithm>
#include <string>

#include "util/hash_util.h"
#include "util/string_heap.h"

namespace tpl::sql {

class Timestamp;

//===----------------------------------------------------------------------===//
//
// Dates
//
//===----------------------------------------------------------------------===//

static constexpr int64_t kMicroSecondsPerSecond = 1000 * 1000;
static constexpr int64_t kMicroSecondsPerMinute = 60UL * kMicroSecondsPerSecond;
static constexpr int64_t kMicroSecondsPerHour = 60UL * kMicroSecondsPerMinute;
static constexpr int64_t kMicroSecondsPerDay = 24UL * kMicroSecondsPerHour;

/**
 * A SQL date.
 */
class Date {
 public:
  using NativeType = int32_t;

  /**
   * Empty constructor.
   */
  Date() = default;

  /**
   * @return A string representation of this date in the form "YYYY-MM-MM".
   */
  std::string ToString() const;

  /**
   * @return The year of this date.
   */
  int32_t ExtractYear() const;

  /**
   * @return The month of this date.
   */
  int32_t ExtractMonth() const;

  /**
   * @return The day of this date.
   */
  int32_t ExtractDay() const;

  /**
   * Convert this date object into its year, month, and day parts.
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   */
  void ExtractComponents(int32_t *year, int32_t *month, int32_t *day);

  /**
   * Convert this date instance into a timestamp instance.
   * @return The timestamp instance representing this date.
   */
  Timestamp ConvertToTimestamp() const;

  /**
   * Compute the hash value of this date instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this date instance.
   */
  hash_t Hash(const hash_t seed) const { return util::HashUtil::HashCrc(value_, seed); }

  /**
   * @return The hash value of this date instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return True if this date equals @em that date; false otherwise.
   */
  bool operator==(const Date &that) const { return value_ == that.value_; }

  /**
   * @return True if this date is not equal to @em that date; false otherwise.
   */
  bool operator!=(const Date &that) const { return value_ != that.value_; }

  /**
   * @return True if this data occurs before @em that date; false otherwise.
   */
  bool operator<(const Date &that) const { return value_ < that.value_; }

  /**
   * @return True if this data occurs before or is the same as @em that date; false otherwise.
   */
  bool operator<=(const Date &that) const { return value_ <= that.value_; }

  /**
   * @return True if this date occurs after @em that date; false otherwise.
   */
  bool operator>(const Date &that) const { return value_ > that.value_; }

  /**
   * @return True if this date occurs after or is equal to @em that date; false otherwise.
   */
  bool operator>=(const Date &that) const { return value_ >= that.value_; }

  /**
   * Convert a C-style string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert
   * the first date-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @param len The length of the string.
   * @return The constructed date. May be invalid.
   */
  static Date FromString(const char *str, std::size_t len);

  /**
   * Convert a string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert the
   * first date-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @return The constructed Date. May be invalid.
   */
  static Date FromString(const std::string &str) { return FromString(str.c_str(), str.size()); }

  /**
   * Create a Date instance from a specified year, month, and day.
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return The constructed date. May be invalid.
   */
  static Date FromYMD(int32_t year, int32_t month, int32_t day);

 private:
  friend class Timestamp;
  friend struct DateVal;

  // Private constructor to force static factories.
  explicit Date(NativeType value) : value_(value) {}

 private:
  // Date value
  NativeType value_;
};

//===----------------------------------------------------------------------===//
//
// Timestamps
//
//===----------------------------------------------------------------------===//

/**
 * A SQL timestamp.
 */
class Timestamp {
 public:
  using NativeType = uint64_t;

  /**
   * Empty constructor.
   */
  Timestamp() = default;

  /**
   * @return The year of this timestamp.
   */
  int32_t ExtractYear() const;

  /**
   * @return The month of this timestamp.
   */
  int32_t ExtractMonth() const;

  /**
   * @return The day of this timestamp.
   */
  int32_t ExtractDay() const;

  /**
   * @return The year of this timestamp.
   */
  int32_t ExtractHour() const;

  /**
   * @return The month of this timestamp.
   */
  int32_t ExtractMinute() const;

  /**
   * @return The day of this timestamp.
   */
  int32_t ExtractSecond() const;

  /**
   * @return The milliseconds of this timestamp.
   */
  int32_t ExtractMillis() const;

  /**
   * @return The microseconds of this timestamp.
   */
  int32_t ExtractMicros() const;

  /**
   * @return The day-of-the-week (0-6 Sun-Sat) this timestamp falls on.
   */
  int32_t ExtractDayOfWeek() const;

  /**
   * @return THe day-of-the-year this timestamp falls on.
   */
  int32_t ExtractDayOfYear() const;

  /**
   * Extract all components of this timestamp
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   * @param[out] hour The hour corresponding to this date.
   * @param[out] min The minute corresponding to this date.
   * @param[out] sec The second corresponding to this date.
   */
  void ExtractComponents(int32_t *year, int32_t *month, int32_t *day, int32_t *hour, int32_t *min,
                         int32_t *sec);

  /**
   * Convert this timestamp instance into a date instance.
   * @return The date instance representing this timestamp.
   */
  Date ConvertToDate() const;

  /**
   * Compute the hash value of this timestamp instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this timestamp instance.
   */
  hash_t Hash(const hash_t seed) const { return util::HashUtil::HashCrc(value_, seed); }

  /**
   * @return The hash value of this timestamp instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return A string representation of timestamp in the form "YYYY-MM-DD HH:MM:SS.ZZZ"
   */
  std::string ToString() const;

  /**
   * @return True if this timestamp equals @em that timestamp; false otherwise.
   */
  bool operator==(const Timestamp &that) const { return value_ == that.value_; }

  /**
   * @return True if this timestamp is not equal to @em that timestamp; false otherwise.
   */
  bool operator!=(const Timestamp &that) const { return value_ != that.value_; }

  /**
   * @return True if this data occurs before @em that timestamp; false otherwise.
   */
  bool operator<(const Timestamp &that) const { return value_ < that.value_; }

  /**
   * @return True if this data occurs before or is the same as @em that timestamp; false otherwise.
   */
  bool operator<=(const Timestamp &that) const { return value_ <= that.value_; }

  /**
   * @return True if this timestamp occurs after @em that timestamp; false otherwise.
   */
  bool operator>(const Timestamp &that) const { return value_ > that.value_; }

  /**
   * @return True if this timestamp occurs after or is equal to @em that timestamp; false otherwise.
   */
  bool operator>=(const Timestamp &that) const { return value_ >= that.value_; }

  /**
   * Convert a C-style string of the form "YYYY-MM-DD HH::MM::SS" into a timestamp. Will attempt to
   * convert the first timestamp-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @param len The length of the string.
   * @return The constructed Timestamp. May be invalid.
   */
  static Timestamp FromString(const char *str, std::size_t len);

  /**
   * Convert a string of the form "YYYY-MM-DD HH::MM::SS" into a timestamp. Will attempt to convert
   * the first timestamp-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @return The constructed Timestamp. May be invalid.
   */
  static Timestamp FromString(const std::string &str) {
    return FromString(str.c_str(), str.size());
  }

  /**
   * Given year, month, day, hour, minute, second components construct a TPL timestamp. If any
   * component is invalid, this will throw an exception.
   * @param year The year.
   * @param month The month.
   * @param day The day.
   * @param hour The hour.
   * @param min The minute.
   * @param sec The second.
   * @return The constructed timestamp if valid.
   */
  static Timestamp FromYMDHMS(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                              int32_t sec);

 private:
  friend class Date;
  friend struct TimestampVal;

  explicit Timestamp(NativeType value) : value_(value) {}

 private:
  // Timestamp value
  NativeType value_;
};

//===----------------------------------------------------------------------===//
//
// Fixed point decimals
//
//===----------------------------------------------------------------------===//

/**
 * A generic fixed point decimal value. This only serves as a storage container for decimals of
 * various sizes. Operations on decimals require a precision and scale.
 * @tparam T The underlying native data type sufficiently large to store decimals of a
 *           pre-determined scale
 */
template <typename T>
class Decimal {
 public:
  using NativeType = T;

  /**
   * Create a decimal value using the given raw underlying encoded value.
   * @param value The value to set this decimal to.
   */
  explicit Decimal(const T &value) : value_(value) {}

  /**
   * @return The raw underlying encoded decimal value.
   */
  operator T() const { return value_; }  // NOLINT

  /**
   * Compute the hash value of this decimal instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this decimal instance.
   */
  hash_t Hash(const hash_t seed) const { return util::HashUtil::HashCrc(value_); }

  /**
   * @return The hash value of this decimal instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * Add the encoded decimal value @em that to this decimal value.
   * @param that The value to add.
   * @return This decimal value.
   */
  const Decimal<T> &operator+=(const T &that) {
    value_ += that;
    return *this;
  }

  /**
   * Subtract the encoded decimal value @em that from this decimal value.
   * @param that The value to subtract.
   * @return This decimal value.
   */
  const Decimal<T> &operator-=(const T &that) {
    value_ -= that;
    return *this;
  }

  /**
   * Multiply the encoded decimal value @em that with this decimal value.
   * @param that The value to multiply by.
   * @return This decimal value.
   */
  const Decimal<T> &operator*=(const T &that) {
    value_ *= that;
    return *this;
  }

  /**
   * Divide this decimal value by the encoded decimal value @em that.
   * @param that The value to divide by.
   * @return This decimal value.
   */
  const Decimal<T> &operator/=(const T &that) {
    value_ /= that;
    return *this;
  }

  /**
   * Modulo divide this decimal value by the encoded decimal value @em that.
   * @param that The value to modulus by.
   * @return This decimal value.
   */
  const Decimal<T> &operator%=(const T &that) {
    value_ %= that;
    return *this;
  }

 private:
  // The encoded decimal value
  T value_;
};

using Decimal32 = Decimal<int32_t>;
using Decimal64 = Decimal<int64_t>;
using Decimal128 = Decimal<int128_t>;

//===----------------------------------------------------------------------===//
//
// Variable-length values
//
//===----------------------------------------------------------------------===//

/**
 * A VarlenEntry is a cheap handle to variable length buffer allocated and owned by another entity.
 */
class VarlenEntry {
 public:
  // Length of the string prefix (in bytes) that is inlined directly into this structure.
  static constexpr uint32_t kPrefixLength = 4;

  /**
   * Constructs a new out-lined varlen entry. The varlen DOES NOT take ownership of the content, but
   * will only store a pointer to it. It is the caller's responsibility to ensure the content
   * outlives this varlen entry.
   *
   * @param content A pointer to the content.
   * @param size The length of the content, in bytes (no C-style nul-terminator).
   * @return A constructed VarlenEntry object.
   */
  static VarlenEntry Create(const byte *content, uint32_t size) {
    VarlenEntry result;
    result.size_ = size;

    if (size <= GetInlineThreshold()) {
      // Small string: just store the prefix. But, first zero initialize prefix
      // to ensure strings smaller than 4 still have an equal prefix.
      std::memset(result.prefix_, 0, GetPrefixSize());
      if (size != 0) {
        std::memcpy(result.prefix_, content, size);
      }
    } else {
      // Large string: copy prefix and store pointer.
      std::memcpy(result.prefix_, content, kPrefixLength);
      result.content_ = content;
    }

    return result;
  }

  /**
   * Construct a new varlen entry whose contents match the provided string. The varlen DOES NOT
   * take ownership of the content, but it will store a pointer to it if it cannot apply a small
   * string optimization. It is the caller's responsibility to ensure the content outlives this
   * varlen entry.
   *
   * @param str The input string.
   * @return A constructed VarlenEntry object.
   */
  static VarlenEntry Create(std::string_view str) {
    return Create(reinterpret_cast<const byte *>(str.data()), str.length());
  }

  /**
   * @return The maximum size of the varlen field, in bytes, that can be inlined within the object.
   * Any objects that are larger need to be stored as a pointer to a separate buffer.
   */
  static constexpr uint32_t GetInlineThreshold() { return sizeof(VarlenEntry) - sizeof(uint32_t); }

  /**
   * @return length of the prefix of the varlen stored in the object for execution engine, if the
   * varlen entry is not inlined.
   */
  static constexpr uint32_t GetPrefixSize() { return kPrefixLength; }

  /**
   * @return The size of the variable-length string in bytes.
   */
  uint32_t GetSize() const { return size_; }

  /**
   * @return True if the entire string is inlined; false otherwise.
   */
  bool IsInlined() const { return GetSize() <= GetInlineThreshold(); }

  /**
   * @return A pointer to the inlined prefix string of this variable-length string.
   */
  const byte *GetPrefix() const { return prefix_; }

  /**
   * @return A pointer to the contents of this variable-length string.
   */
  const byte *GetContent() const { return IsInlined() ? prefix_ : content_; }

  /**
   * Compute the hash value of this variable-length string instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this string instance.
   */
  hash_t Hash(hash_t seed) const;

  /**
   * @return The hash value of this variable-length string.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return A zero-copy view of the VarlenEntry as an immutable string that allows use with
   *         convenient STL functions
   * @warning It is the user's responsibility to ensure that std::string_view does not outlive
   *          the VarlenEntry
   */
  std::string_view GetStringView() const {
    return std::string_view(reinterpret_cast<const char *const>(GetContent()), GetSize());
  }

  /**
   * Compare two strings ONLY for equality or inequality only.
   * @tparam EqualCheck
   * @param left The first string.
   * @param right The second string.
   * @return 0 if equal; any non-zero value otherwise.
   */
  template <bool EqualityCheck>
  static bool CompareEqualOrNot(const VarlenEntry &left, const VarlenEntry &right) {
    // Compare the size and prefix in one fell swoop.
    if (std::memcmp(&left, &right, sizeof(size_) + GetPrefixSize()) == 0) {
      // Prefix and length are equal.
      if (left.IsInlined()) {
        if (std::memcmp(left.prefix_, right.prefix_, left.size_) == 0) {
          return EqualityCheck ? true : false;
        }
      } else {
        if (std::memcmp(left.content_, right.content_, left.size_) == 0) {
          return EqualityCheck ? true : false;
        }
      }
    }
    // Not equal.
    return EqualityCheck ? false : true;
  }

  /**
   * Compare two strings. Returns:
   * < 0 if left < right
   *  0  if left == right
   * > 0 if left > right
   *
   * @param left The first string.
   * @param right The second string.
   * @return The appropriate signed value indicating comparison order.
   */
  static int32_t Compare(const VarlenEntry &left, const VarlenEntry &right) {
    const auto min_len = std::min(left.GetSize(), right.GetSize());
    const auto result = std::memcmp(left.GetContent(), right.GetContent(), min_len);
    return result != 0 ? result : left.GetSize() - right.GetSize();
  }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator==(const VarlenEntry &that) const { return CompareEqualOrNot<true>(*this, that); }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator!=(const VarlenEntry &that) const { return CompareEqualOrNot<false>(*this, that); }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator<(const VarlenEntry &that) const { return Compare(*this, that) < 0; }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator<=(const VarlenEntry &that) const { return Compare(*this, that) <= 0; }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator>(const VarlenEntry &that) const { return Compare(*this, that) > 0; }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator>=(const VarlenEntry &that) const { return Compare(*this, that) >= 0; }

 private:
  // The size of the contents
  int32_t size_;
  // A small prefix for the string. Immediately valid when content is inlined, but used when content
  // is not inlined as well.
  byte prefix_[kPrefixLength];
  // Pointer to the content when not inlined
  union {
    byte inlined_[sizeof(byte *)];
    const byte *content_;
  };
};

static_assert(sizeof(VarlenEntry) == 16U, "VarlenEntry expected to be 16 bytes");

/**
 * A container for varlens.
 */
class VarlenHeap {
 public:
  /**
   * Allocate memory from the heap whose contents will be filled in by the user BEFORE creating a
   * varlen.
   * @param len The length of the varlen to allocate.
   * @return The character byte array.
   */
  [[nodiscard]] char *PreAllocate(std::size_t len) { return heap_.Allocate(len); }

  /**
   * Allocate a varlen from this heap whose contents are the same as the input string.
   * @param str The string to copy into the heap.
   * @param len The length of the input string.
   * @return A varlen.
   */
  [[nodiscard]] VarlenEntry AddVarlen(const char *str, std::size_t len) {
    auto *content = heap_.AddString(std::string_view(str, len));
    return VarlenEntry::Create(reinterpret_cast<byte *>(content), len);
  }

  /**
   * Allocate and return a varlen from this heap whose contents as the same as the input string.
   * @param string The string to copy into the heap.
   * @return A varlen.
   */
  [[nodiscard]] VarlenEntry AddVarlen(const std::string &string) {
    return AddVarlen(string.c_str(), string.length());
  }

  /**
   * Add a copy of the given varlen into this heap.
   * @param other The varlen to copy into this heap.
   * @return A new varlen entry.
   */
  [[nodiscard]] VarlenEntry AddVarlen(const VarlenEntry &other) {
    return AddVarlen(reinterpret_cast<const char *>(other.GetContent()), other.GetSize());
  }

  /**
   * Destroy all heap-allocated varlens.
   */
  void Destroy() { heap_.Destroy(); }

 private:
  // Internal heap of strings
  util::StringHeap heap_;
};

/**
 * Simple structure representing a blob.
 */
class Blob {
 public:
  /**
   * Crete an empty blob reference.
   */
  Blob() : data_(nullptr), size_(0) {}

  /**
   * Create a reference to an existing blob.
   * @param data The blob data.
   * @param size The size of the blob.
   */
  Blob(byte *data, std::size_t size) : data_(data), size_(size) {}

  /**
   * @return A const-view of the raw blob data.
   */
  const byte *GetData() const { return data_; }

  /**
   * @return The size of the blob in bytes.
   */
  std::size_t GetSize() const { return size_; }

  /**
   * @return The hash value for this BLOB using the given seed.
   */
  hash_t Hash(hash_t seed) const;

  /**
   * @return The hash value of this BLOB.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * Compare two strings. Returns:
   * < 0 if left < right
   *  0  if left == right
   * > 0 if left > right
   *
   * @param left The first blob.
   * @param right The second blob.
   * @return The appropriate signed value indicating comparison order.
   */
  static int32_t Compare(const Blob &left, const Blob &right) {
    const std::size_t min_len = std::min(left.GetSize(), right.GetSize());
    const int32_t result =
        min_len == 0 ? 0 : std::memcmp(left.GetData(), right.GetData(), left.GetSize());
    if (result != 0) {
      return result;
    }
    return left.GetSize() - right.GetSize();
  }

  /**
   * @return True if this blob is byte-for-byte equivalent to @em that blob; false otherwise.
   */
  bool operator==(const Blob &that) const {
    return size_ == that.size_ && std::memcmp(data_, that.data_, size_) == 0;
  }

  /**
   * @return True if this blob is byte-for-byte not-equal-to @em that blob; false otherwise.
   */
  bool operator!=(const Blob &that) const { return !(*this == that); }

  /**
   * @return True if this blob is byte-for-byte less-than @em that blob; false otherwise.
   */
  bool operator<(const Blob &that) const { return Compare(*this, that) < 0; }

  /**
   * @return True if this blob is byte-for-byte less-than-or-equal-to @em that blob; false
   *         otherwise.
   */
  bool operator<=(const Blob &that) const { return Compare(*this, that) <= 0; }

  /**
   * @return True if this blob is byte-for-byte greater than @em that blob; false otherwise.
   */
  bool operator>(const Blob &that) const { return Compare(*this, that) > 0; }

  /**
   * @return True if this blob is byte-for-byte greater-than-or-equal-to @em that blob; false
   *         otherwise.
   */
  bool operator>=(const Blob &that) const { return Compare(*this, that) >= 0; }

 private:
  // Root
  byte *data_;

  // Length
  std::size_t size_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline Timestamp Date::ConvertToTimestamp() const {
  return Timestamp(value_ * kMicroSecondsPerDay);
}

inline Date Timestamp::ConvertToDate() const { return Date(value_ / kMicroSecondsPerDay); }

}  // namespace tpl::sql
