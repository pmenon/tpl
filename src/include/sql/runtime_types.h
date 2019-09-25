#pragma once

#include <algorithm>
#include <string>

#include "util/hash_util.h"
#include "util/string_heap.h"

namespace tpl::sql {

// ---------------------------------------------------------
//
// Dates
//
// ---------------------------------------------------------

/**
 * A SQL date.
 */
class Date {
 public:
  /**
   * Empty constructor.
   */
  Date() = default;

  /**
   * Is this a valid date?
   * @return True if this is a valid date instance; false otherwise.
   */
  bool IsValid() const noexcept;

  /**
   * Convert this date object into a string of the form "YYYY-MM-DD"
   * @return The stringification of this date object.
   */
  std::string ToString() const;

  /**
   * Return the year corresponding to this date.
   * @return The year of the date.
   */
  uint32_t ExtractYear() const noexcept;

  /**
   * Return the month corresponding to this date.
   * @return The month of the date.
   */
  uint32_t ExtractMonth() const noexcept;

  /**
   * Return the day corresponding to this date.
   * @return The day of the date.
   */
  uint32_t ExtractDay() const noexcept;

  /**
   * Convert this date object into its year, month, and day parts.
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   */
  void Convert(uint32_t *year, uint32_t *month, uint32_t *day) {
    *year = ExtractYear();
    *month = ExtractMonth();
    *day = ExtractDay();
  }

  /**
   * Equality comparison.
   * @return True if this date equals @em that date.
   */
  bool operator==(const Date &that) const noexcept { return value_ == that.value_; }

  /**
   * Inequality comparison.
   * @return True if this date is not equal to @em that date.
   */
  bool operator!=(const Date &that) const noexcept { return value_ != that.value_; }

  /**
   * Less-than comparison.
   * @return True if this data occurs before @em that date.
   */
  bool operator<(const Date &that) const noexcept { return value_ < that.value_; }

  /**
   * Less-than-or-equal-to comparison.
   * @return True if this data occurs before or is the same as @em that date.
   */
  bool operator<=(const Date &that) const noexcept { return value_ <= that.value_; }

  /**
   * Greater-than comparison.
   * @return True if this date occurs after @em that date.
   */
  bool operator>(const Date &that) const noexcept { return value_ > that.value_; }

  /**
   * Greater-than-or-equal-to comparison.
   * @return True if this date occurs after or is equal to @em that date.
   */
  bool operator>=(const Date &that) const noexcept { return value_ >= that.value_; }

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
  static Date FromYMD(uint32_t year, uint32_t month, uint32_t day);

  /**
   * Is the date corresponding to the given year, month, and day a valid date?
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return True if valid date.
   */
  static bool IsValidDate(uint32_t year, uint32_t month, uint32_t day) {
    return FromYMD(year, month, day).IsValid();
  }

  /**
   * Hash the date.
   * @return The hash value for this date instance.
   */
  hash_t Hash() const noexcept { return util::HashUtil::Hash(value_); }

 private:
  friend struct DateVal;

  // Private constructor to force static factories.
  explicit Date(uint32_t value) : value_(value) {}

 private:
  // Date value
  uint32_t value_;
};

// ---------------------------------------------------------
//
// Variable-length values
//
// ---------------------------------------------------------

/**
 * A VarlenEntry is a cheap handle to variable length buffer allocated and owned by another entity.
 */
class VarlenEntry {
 public:
  // Prefix length inlined in the varlen structure.
  static constexpr uint32_t kPrefixLength = 4;
  // Length clamp used to avoid std::min().
  static constexpr uint32_t kPrefixLengthClamp = kPrefixLength - 1;

  // Ensure assumption
  static_assert(util::MathUtil::IsPowerOf2(kPrefixLength));

  /**
   * Constructs a new out-lined varlen entry. The varlen DOES NOT take ownership of the content, but
   * will only store a pointer to it. It is the caller's responsibility to ensure the content
   * outlives this varlen entry.
   *
   * @param content A Pointer to the varlen content itself.
   * @param size The length of the varlen content, in bytes (no C-style nul-terminator).
   * @return A constructed VarlenEntry object.
   */
  static VarlenEntry Create(const byte *content, uint32_t size) {
    VarlenEntry result;
    result.size_ = size;

    // If the size is small enough for the content to be inlined, apply that optimization. If not,
    // store a prefix of the content inline and point to the bigger content.

    if (size <= GetInlineThreshold()) {
      std::memcpy(result.prefix_, content, size);
    } else {
      std::memcpy(result.prefix_, content, kPrefixLength);
      result.content_ = content;
    }

    return result;
  }

  /**
   * @return The maximum size of the varlen field, in bytes, that can be inlined within the object.
   * Any objects that are larger need to be stored as a pointer to a separate buffer.
   */
  static constexpr uint32_t GetInlineThreshold() { return sizeof(VarlenEntry) - kPrefixLength; }

  /**
   * @return length of the prefix of the varlen stored in the object for execution engine, if the
   * varlen entry is not inlined.
   */
  static constexpr uint32_t GetPrefixSize() { return kPrefixLength; }

  /**
   * @return size of the varlen value stored in this entry, in bytes.
   */
  uint32_t GetSize() const { return size_; }

  /**
   * @return whether the content is inlined or not.
   */
  bool IsInlined() const { return GetSize() <= GetInlineThreshold(); }

  /**
   * @return pointer to the stored prefix of the varlen entry
   */
  const byte *GetPrefix() const { return prefix_; }

  /**
   * @return pointer to the varlen entry contents.
   */
  const byte *GetContent() const { return IsInlined() ? prefix_ : content_; }

  /**
   * @return The hash of this varlen.
   */
  hash_t Hash() const noexcept;

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

    if (min_len < GetPrefixSize()) {
      int32_t result = std::memcmp(left.GetPrefix(), right.GetPrefix(), min_len);
      return result != 0 ? result : left.GetSize() - right.GetSize();
    }

    // Full prefix match?
    int32_t result = std::memcmp(left.GetPrefix(), right.GetPrefix(), GetPrefixSize());
    if (result != 0) {
      return result;
    }

    // Content match (minus prefix)
    result = std::memcmp(left.GetContent() + GetPrefixSize(), right.GetContent() + GetPrefixSize(),
                         min_len - GetPrefixSize());
    return result != 0 ? result : left.GetSize() - right.GetSize();
  }

  bool operator==(const VarlenEntry &that) const noexcept { return Compare(*this, that) == 0; }
  bool operator!=(const VarlenEntry &that) const noexcept { return Compare(*this, that) != 0; }
  bool operator<(const VarlenEntry &that) const noexcept { return Compare(*this, that) < 0; }
  bool operator<=(const VarlenEntry &that) const noexcept { return Compare(*this, that) <= 0; }
  bool operator>(const VarlenEntry &that) const noexcept { return Compare(*this, that) > 0; }
  bool operator>=(const VarlenEntry &that) const noexcept { return Compare(*this, that) >= 0; }

 private:
  // The size of the contents
  int32_t size_;
  // A small prefix for the string. Immediately valid when content is inlined, but used when content
  // is not inlined as well.
  byte prefix_[kPrefixLength];
  // Pointer to the content when not inlined
  const byte *content_;
};

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
  char *PreAllocate(std::size_t len) { return heap_.Allocate(len); }

  /**
   * Allocate a varlen from this heap whose contents are the same as the input string.
   * @param str The string to copy into the heap.
   * @param len The length of the input string.
   * @return A varlen.
   */
  VarlenEntry AddVarlen(const char *str, std::size_t len) {
    auto *content = heap_.AddString(std::string_view(str, len));
    return VarlenEntry::Create(reinterpret_cast<byte *>(content), len);
  }

  /**
   * Allocate and return a varlen from this heap whose contents as the same as the input string.
   * @param string The string to copy into the heap.
   * @return A varlen.
   */
  VarlenEntry AddVarlen(const std::string &string) {
    return AddVarlen(string.c_str(), string.length());
  }

  /**
   * Add a copy of the given varlen into this heap.
   * @param other The varlen to copy into this heap.
   * @return A new varlen entry.
   */
  VarlenEntry AddVarlen(const VarlenEntry &other) {
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

}  // namespace tpl::sql
