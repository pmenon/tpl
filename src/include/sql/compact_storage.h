#pragma once

#include <type_traits>
#include <vector>

#include "sql/sql.h"

namespace tpl::sql {

/**
 * A class that compactly stores data into a memory space. This class minimizes wasted space when
 * serializing data into a buffer by potentially reordering elements from a provided input schema.
 *
 * To use, construct an instance providing the types of all the columns you intend to serialize.
 * Then, write each column/attribute value individually by providing (1) the index of the element
 * you want to write, (2) a pointer to the buffer space for the whole row, (3) the value of the
 * element and (4) a null flag.
 *
 * To read back data, provide the same information as in Write(), but provide pointers to the
 * value as output parameters that will be written into.
 *
 * Use:
 * @code
 * auto storage = CompactStorage({TypeId::TinyInt, TypeId::BigInt, TypeId::Integer});
 * // storage will internally re-arrange the provided schema to optimize memory.
 * storage.Write<int8_t,false>(0,p,100,false);
 * storage.Write<int64_t,true>(1,p,11,true);
 * storage.Write<int32_t,true>(2,p,44,false);
 * // Now, we're serialized [0,NULL,44] into the row buffer.
 * int8_t  v1;
 * int64_t v2;
 * int32_t v3;
 * bool v1n, v2n, v3n;
 * storage.Read<int8_t,false>(0,p,&v1,&v1n);
 * storage.Read<int64_t,true>(0,p,&v1,&v1n);
 * storage.Read<int32_t,true>(0,p,&v1,&v1n);
 * // v1=100 and v3=44, v1n=v3n=false, v2n=true.
 * // Easy.
 *
 * @endcode
 */
class CompactStorage {
  // Helper class to read and write to compact memory space. Needed so we can
  // specialize how different types are stored.
  template <typename T, typename Enable = void>
  struct StorageHelper;

 public:
  /**
   * Empty.
   */
  CompactStorage() = default;

  /**
   * Create an instance for a row with the provided schema.
   * @param schema The schema of the row to be stored.
   */
  explicit CompactStorage(const std::vector<TypeId> &schema);

  /**
   * @return The number of elements this storage is configured to handle.
   */
  uint32_t GetNumElements() const;

  /**
   * @return The preferred alignment for this storage.
   */
  std::size_t GetPreferredAlignment() const;

  /**
   * @return The total number of bytes needed by this storage.
   */
  std::size_t GetRequiredSize() const;

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
  void Write(uint32_t index, byte *ptr, const T &val, bool null) const;

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
  void Read(uint32_t index, const byte *ptr, T *val, bool *null) const;

 private:
  // Write the NULL indication bit at the given index.
  void SetNullIndicator(byte *null_bitmap, uint32_t index, bool null) const;
  // Read the NULL indication bit at the given index.
  bool GetNullIndicator(const byte *null_bitmap, uint32_t index) const;
  // What block does the element with the given index belong to?
  static std::size_t NullBlockIndex(std::size_t pos) noexcept { return pos / 8; }
  // What bit position in a block does the element with the index belong to?
  static std::size_t NullBitIndex(std::size_t pos) noexcept { return pos % 8; }
  // The one-hot block mask for the element at the given position.
  static byte NullMask(std::size_t pos) noexcept { return byte{1} << NullBitIndex(pos); }

 private:
  // Preferred alignment.
  std::size_t preferred_alignment_;
  // Information for all slots.
  std::vector<std::size_t> offsets_;
  // Byte-offset where the NULL bitmap is.
  std::size_t null_bitmap_offset_;
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
  static constexpr void Read(const byte *p, T *val, UNUSED bool null) noexcept {
    *val = *reinterpret_cast<const T *>(p);
  }
};

inline void CompactStorage::SetNullIndicator(byte *null_bitmap, uint32_t index, bool null) const {
  null_bitmap[NullBlockIndex(index)] |= byte{null} << (NullBitIndex(index));
}

inline bool CompactStorage::GetNullIndicator(const byte *null_bitmap, uint32_t index) const {
  return (null_bitmap[NullBlockIndex(index)] & NullMask(index)) != byte{0};
}

template <typename T, bool Nullable>
inline void CompactStorage::Write(uint32_t index, byte *ptr, const T &val, bool null) const {
  if constexpr (Nullable) SetNullIndicator(ptr + null_bitmap_offset_, index, null);
  StorageHelper<T>::Write(&ptr[offsets_[index]], val, null);
}

template <typename T, bool Nullable>
inline void CompactStorage::Read(uint32_t index, const byte *ptr, T *val, bool *null) const {
  if constexpr (Nullable) *null = GetNullIndicator(ptr + null_bitmap_offset_, index);
  StorageHelper<T>::Read(&ptr[offsets_[index]], val, null);
}

}  // namespace tpl::sql
