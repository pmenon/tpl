#pragma once

#include <type_traits>

#include "common/common.h"
#include "sql/sql.h"

namespace tpl::sql {

/**
 * A class that compactly stores data into a memory space.
 */
class CompactStorage : public AllStatic {
  /**
   * Helper class to read and write to compact memory space. Needed so we can specialize how
   * different types are stored.
   */
  template <typename T, typename Enable = void>
  struct StorageHelper;

 public:
  /**
   * Write the value @em val into the memory buffer @em p. The value is associated to the element
   * at the index @em index in the row's schema. The @em Nullable template parameter determines
   * if the input value is allowed to be NULL-able, and the @em null flag represents if the value
   * actually is NULL.
   * @tparam T The primitive type to store.
   * @tparam Nullable Boolean indicating of the input values are allowed to be NULL.
   * @param index The index in the input schema whose attribute/column we're to store.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param val The value to store.
   * @param null The NULL indication flag. Garbage is the value is not NULLable.
   */
  template <typename T, bool Nullable>
  static void Write(byte *ptr, byte nulls[], uint32_t index, const T &val, bool null);

  /**
   * Read the value of the column/attribute at index @em index in the row pointed to by the pointer
   * @em p. The value is copied into the output parameter @em val. If the template parameter
   * @em Nullable indicates that the value can be NULL, the output parameter @em null is updated
   * with the values NULL indication.
   * @tparam T The primitive type to store.
   * @tparam Nullable Boolean indicating of the value is allowed to be NULL.
   * @param index The index in the input schema whose attribute/column we're to read.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param[out] val Where the row's column/attribute data is stored.
   * @param[out] null The NULL indication flag.
   */
  template <typename T, bool Nullable>
  static const T *Read(const byte *ptr, const byte nulls[], uint32_t index, bool *null);

 private:
  // What block does the element with the given index belong to?
  static std::size_t NullBlockIndex(std::size_t pos) noexcept { return pos / 8; }
  // What bit position in a block does the element with the index belong to?
  static std::size_t NullBitIndex(std::size_t pos) noexcept { return pos % 8; }
  // The one-hot block mask for the element at the given position.
  static byte NullMask(std::size_t pos) noexcept { return byte{1} << NullBitIndex(pos); }

  // Write the NULL indication bit at the given index.
  static void SetNullIndicator(byte *null_bitmap, uint32_t index, bool null);
  // Read the NULL indication bit at the given index.
  static bool GetNullIndicator(const byte *null_bitmap, uint32_t index);
};

// ---------------------------------------------------------
//
// Implementation below
//
// ---------------------------------------------------------

// Inlined here for performance. Please don't move unless you're certain of what
// you're doing.
//
// Note:
// We always blindly write the value regardless of whether it's NULL or not.
// This may not be optimal. We can control this behavior by specializing
// StorageHelper and using it as a trait. Add methods to it to refine precisely
// how to serialize data into memory for your type.

template <typename T>
struct CompactStorage::StorageHelper<
    T, std::enable_if_t<std::is_fundamental_v<T> || std::is_same_v<T, Date> ||
                        std::is_same_v<T, Timestamp> || std::is_same_v<T, Decimal32> ||
                        std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128> ||
                        std::is_same_v<T, VarlenEntry>>> {
  // Write a fundamental type.
  static constexpr void Write(byte *p, T val, UNUSED bool null) noexcept {
    *reinterpret_cast<T *>(p) = val;
  }

  // Read a fundamental type.
  static constexpr const T *Read(const byte *p, UNUSED bool *null) noexcept {
    return reinterpret_cast<const T *>(p);
  }
};

inline void CompactStorage::SetNullIndicator(byte *null_bitmap, uint32_t index, bool null) {
  null_bitmap[NullBlockIndex(index)] |= byte{null} << (NullBitIndex(index));
}

inline bool CompactStorage::GetNullIndicator(const byte *null_bitmap, uint32_t index) {
  return (null_bitmap[NullBlockIndex(index)] & NullMask(index)) != byte{0};
}

template <typename T, bool Nullable>
inline void CompactStorage::Write(byte *ptr, byte nulls[], uint32_t index, const T &val,
                                  bool null) {
  if constexpr (Nullable) SetNullIndicator(nulls, index, null);
  StorageHelper<T>::Write(ptr, val, null);
}

template <typename T, bool Nullable>
inline const T *CompactStorage::Read(const byte *ptr, const byte nulls[], uint32_t index,
                                     bool *null) {
  if constexpr (Nullable) *null = GetNullIndicator(nulls, index);
  return StorageHelper<T>::Read(ptr, null);
}

}  // namespace tpl::sql
