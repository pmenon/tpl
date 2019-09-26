#pragma once

#include <immintrin.h>

#include "common/common.h"
#include "common/macros.h"
#include "util/simd/types.h"

namespace tpl::util::simd {

#define USE_GATHER 1

// ---------------------------------------------------------
// Vec256b Definition
// ---------------------------------------------------------

/// A 512-bit SIMD register vector. This is a purely internal class that holds
/// common functions for other user-visible vector classes
class Vec512b {
 public:
  Vec512b() = default;
  explicit Vec512b(const __m512i &reg) : reg_(reg) {}

  // Type-cast operator so that Vec*'s can be used directly with intrinsics
  operator __m512i() const { return reg_; }

  void StoreUnaligned(void *ptr) const { _mm512_storeu_si512(ptr, reg()); }

  void StoreAligned(void *ptr) const { _mm512_store_si512(ptr, reg()); }

 protected:
  const __m512i &reg() const { return reg_; }

 protected:
  __m512i reg_;
};

// ---------------------------------------------------------
// Vec8 Definition
// ---------------------------------------------------------

/// A vector of eight 64-bit values
class Vec8 : public Vec512b {
 public:
  Vec8() = default;
  explicit Vec8(int64_t val) { reg_ = _mm512_set1_epi64(val); }
  explicit Vec8(const __m512i &reg) : Vec512b(reg) {}
  Vec8(int64_t val1, int64_t val2, int64_t val3, int64_t val4, int64_t val5, int64_t val6,
       int64_t val7, int64_t val8) {
    reg_ = _mm512_setr_epi64(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  static constexpr uint32_t Size() { return 8; }

  /// Load and sign-extend eight 32-bit values stored contiguously from the
  /// input pointer array.
  Vec8 &Load(const int32_t *ptr);
  Vec8 &Load(const uint32_t *ptr) { return Load(reinterpret_cast<const int32_t *>(ptr)); }

  /// Load and sign-extend eight 64-bit values stored contiguously from the
  /// input pointer array.
  Vec8 &Load(const int64_t *ptr);
  Vec8 &Load(const uint64_t *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }

#ifdef __APPLE__
  static_assert(sizeof(long) == sizeof(int64_t), "On MacOS, long isn't 64-bits!");
  Vec8 &Load(const long *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }
  Vec8 &Load(const unsigned long *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }
#endif

  template <typename T>
  Vec8 &Gather(const T *ptr, const Vec8 &pos);

  /// Truncate eight 64-bit integers to eight 8-bit integers and store the
  /// result into the output array.
  void Store(int8_t *arr) const;
  void Store(uint8_t *arr) const { Store(reinterpret_cast<int8_t *>(arr)); }

  /// Truncate eight 64-bit integers to eight 16-bit integers and store the
  /// result into the output array.
  void Store(int16_t *arr) const;
  void Store(uint16_t *arr) const { Store(reinterpret_cast<int16_t *>(arr)); }

  /// Truncate eight 64-bit integers to eight 32-bit integers and store the
  /// result into the output array.
  void Store(int32_t *arr) const;
  void Store(uint32_t *arr) const { Store(reinterpret_cast<int32_t *>(arr)); }

  /// Store the eight 64-bit integers in this vector into the output array.
  void Store(int64_t *arr) const;
  void Store(uint64_t *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }

#ifdef __APPLE__
  static_assert(sizeof(long) == sizeof(int64_t), "On MacOS, long isn't 64-bits!");
  void Store(long *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }
  void Store(unsigned long *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }
#endif

  bool AllBitsAtPositionsSet(const Vec8 &mask) const;

  int64_t Extract(uint32_t index) const {
    TPL_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(64) int64_t x[Size()];
    Store(x);
    return x[index & 7];
  }

  int64_t operator[](uint32_t index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec8 Implementation
// ---------------------------------------------------------

inline Vec8 &Vec8::Load(const int32_t *ptr) {
  auto tmp = _mm256_load_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi32_epi64(tmp);
  return *this;
}

inline Vec8 &Vec8::Load(const int64_t *ptr) {
  reg_ = _mm512_loadu_si512((const __m512i *)ptr);
  return *this;
}

template <typename T>
inline Vec8 &Vec8::Gather(const T *ptr, const Vec8 &pos) {
#if USE_GATHER
  reg_ = _mm512_i64gather_epi64(pos, ptr, 8);
#else
  alignas(64) int64_t x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi64(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]],
                           ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

inline void Vec8::Store(int8_t *arr) const {
  const __mmask8 all(~(uint8_t)0);
  _mm512_mask_cvtepi64_storeu_epi8(reinterpret_cast<void *>(arr), all, reg());
}

inline void Vec8::Store(int16_t *arr) const {
  const __mmask8 all(~(uint8_t)0);
  _mm512_mask_cvtepi64_storeu_epi16(reinterpret_cast<void *>(arr), all, reg());
}

inline void Vec8::Store(int32_t *arr) const {
  const __mmask8 all(~(uint8_t)0);
  _mm512_mask_cvtepi64_storeu_epi32(reinterpret_cast<void *>(arr), all, reg());
}

inline void Vec8::Store(int64_t *arr) const {
  Vec512b::StoreUnaligned(reinterpret_cast<void *>(arr));
}

inline bool Vec8::AllBitsAtPositionsSet(const Vec8 &mask) const {
  return _mm512_testn_epi64_mask(reg(), mask) == 0;
}

// ---------------------------------------------------------
// Vec16 Definition
// ---------------------------------------------------------

class Vec16 : public Vec512b {
 public:
  Vec16() = default;
  explicit Vec16(int32_t val) { reg_ = _mm512_set1_epi32(val); }
  explicit Vec16(const __m512i &reg) : Vec512b(reg) {}
  Vec16(int32_t val1, int32_t val2, int32_t val3, int32_t val4, int32_t val5, int32_t val6,
        int32_t val7, int32_t val8, int32_t val9, int32_t val10, int32_t val11, int32_t val12,
        int32_t val13, int32_t val14, int32_t val15, int32_t val16) {
    reg_ = _mm512_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11,
                             val12, val13, val14, val15, val16);
  }

  static constexpr int Size() { return 16; }

  /// Load and sign-extend 16 8-bit values stored contiguously from the input
  /// pointer array.
  Vec16 &Load(const int8_t *ptr);
  Vec16 &Load(const uint8_t *ptr) { return Load(reinterpret_cast<const int8_t *>(ptr)); }

  /// Load and sign-extend 16 16-bit values stored contiguously from the input
  /// pointer array.
  Vec16 &Load(const int16_t *ptr);
  Vec16 &Load(const uint16_t *ptr) { return Load(reinterpret_cast<const int16_t *>(ptr)); }

  /// Load 16 32-bit values stored contiguously from the input pointer array.
  Vec16 &Load(const int32_t *ptr);
  Vec16 &Load(const uint32_t *ptr) { return Load(reinterpret_cast<const int32_t *>(ptr)); }

  template <typename T>
  Vec16 &Gather(const T *ptr, const Vec16 &pos);

  /// Truncate the 16 32-bit integers in this vector to 16 8-bit integers and
  /// store the result in the output array.
  void Store(int8_t *arr) const;
  void Store(uint8_t *arr) const { Store(reinterpret_cast<int8_t *>(arr)); }

  /// Truncate the 16 32-bit integers in this vector to 16 16-bit integers and
  /// store the result in the output array.
  void Store(int16_t *arr) const;
  void Store(uint16_t *arr) const { Store(reinterpret_cast<int16_t *>(arr)); }

  /// Store the 16 32-bit integers in the output array.
  void Store(int32_t *arr) const;
  void Store(uint32_t *arr) const { Store(reinterpret_cast<int32_t *>(arr)); }

  /// Given a vector of 16 32-bit masks, return true if all corresponding bits
  /// in this vector are set
  bool AllBitsAtPositionsSet(const Vec16 &mask) const;

  int32_t Extract(uint32_t index) const {
    alignas(64) int32_t x[Size()];
    Store(x);
    return x[index & 15];
  }

  int32_t operator[](uint32_t index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec16 Implementation
// ---------------------------------------------------------

inline Vec16 &Vec16::Load(const int8_t *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm512_cvtepi8_epi32(tmp);
  return *this;
}

inline Vec16 &Vec16::Load(const int16_t *ptr) {
  auto tmp = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi16_epi32(tmp);
  return *this;
}

inline Vec16 &Vec16::Load(const int32_t *ptr) {
  reg_ = _mm512_loadu_si512(ptr);
  return *this;
}

template <typename T>
inline Vec16 &Vec16::Gather(const T *ptr, const Vec16 &pos) {
  using signed_t = std::make_signed_t<T>;
  return Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

template <>
inline Vec16 &Vec16::Gather<int8_t>(const int8_t *ptr, const Vec16 &pos) {
  alignas(64) int32_t x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]],
                           ptr[x[6]], ptr[x[7]], ptr[x[8]], ptr[x[9]], ptr[x[10]], ptr[x[11]],
                           ptr[x[12]], ptr[x[13]], ptr[x[14]], ptr[x[15]]);
  return *this;
}

template <>
inline Vec16 &Vec16::Gather<int16_t>(const int16_t *ptr, const Vec16 &pos) {
  alignas(64) int32_t x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]],
                           ptr[x[6]], ptr[x[7]], ptr[x[8]], ptr[x[9]], ptr[x[10]], ptr[x[11]],
                           ptr[x[12]], ptr[x[13]], ptr[x[14]], ptr[x[15]]);
  return *this;
}

template <>
inline Vec16 &Vec16::Gather<int32_t>(const int32_t *ptr, const Vec16 &pos) {
  reg_ = _mm512_i32gather_epi32(pos, ptr, 4);
  return *this;
}

inline void Vec16::Store(int8_t *arr) const {
  const __mmask16 all(~(uint16_t)0);
  _mm512_mask_cvtepi32_storeu_epi8(reinterpret_cast<void *>(arr), all, reg());
}

inline void Vec16::Store(int16_t *arr) const {
  const __mmask16 all(~(uint16_t)0);
  _mm512_mask_cvtepi32_storeu_epi16(reinterpret_cast<void *>(arr), all, reg());
}

inline void Vec16::Store(int32_t *arr) const {
  Vec512b::StoreUnaligned(reinterpret_cast<void *>(arr));
}

inline bool Vec16::AllBitsAtPositionsSet(const Vec16 &mask) const {
  return _mm512_testn_epi32_mask(reg(), mask) == 0;
}

// ---------------------------------------------------------
// Vec8Mask Definition
// ---------------------------------------------------------

class Vec8Mask {
 public:
  Vec8Mask() = default;
  explicit Vec8Mask(const __mmask8 &mask) : mask_(mask) {}

  uint32_t ToPositions(uint32_t *positions, uint32_t offset) const {
    __m512i sequence = _mm512_setr_epi64(0, 1, 2, 3, 4, 5, 6, 7);
    __m512i pos_vec = _mm512_add_epi64(sequence, _mm512_set1_epi64(offset));
    __m128i pos_vec_comp = _mm512_cvtepi64_epi16(pos_vec);
    _mm_mask_compressstoreu_epi16(positions, mask_, pos_vec_comp);
    return __builtin_popcountll(mask_);
  }

  static constexpr int Size() { return 8; }

  bool Extract(uint32_t index) const { return (static_cast<uint32_t>(mask_) >> index) & 1; }

  bool operator[](uint32_t index) const { return Extract(index); }

  operator __mmask8() const { return mask_; }

 private:
  __mmask8 mask_;
};

// ---------------------------------------------------------
// Vec16Mask Definition
// ---------------------------------------------------------

class Vec16Mask {
 public:
  Vec16Mask() = default;
  explicit Vec16Mask(const __mmask16 &mask) : mask_(mask) {}

  static constexpr int Size() { return 16; }

  uint32_t ToPositions(uint32_t *positions, uint32_t offset) const {
    __m512i sequence = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    __m512i pos_vec = _mm512_add_epi32(sequence, _mm512_set1_epi32(offset));
    __m256i pos_vec_comp = _mm512_cvtepi32_epi16(pos_vec);
    _mm256_mask_compressstoreu_epi16(positions, mask_, pos_vec_comp);
    return __builtin_popcountll(mask_);
  }

  bool Extract(uint32_t index) const { return (static_cast<uint32_t>(mask_) >> index) & 1; }

  bool operator[](uint32_t index) const { return Extract(index); }

  operator __mmask16() const { return mask_; }

 private:
  __mmask16 mask_;
};

// ---------------------------------------------------------
// Vec512b Bitwise Operations
// ---------------------------------------------------------

inline Vec512b operator&(const Vec512b &a, const Vec512b &b) {
  return Vec512b(_mm512_and_epi64(a, b));
}

inline Vec512b operator|(const Vec512b &a, const Vec512b &b) {
  return Vec512b(_mm512_or_si512(a, b));
}

inline Vec512b operator^(const Vec512b &a, const Vec512b &b) {
  return Vec512b(_mm512_xor_si512(a, b));
}

// ---------------------------------------------------------
// Vec8 Comparison Operations
// ---------------------------------------------------------

inline Vec8Mask operator>(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpgt_epi64_mask(a, b));
}

inline Vec8Mask operator==(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpeq_epi64_mask(a, b));
}

inline Vec8Mask operator<(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmplt_epi64_mask(a, b));
}

inline Vec8Mask operator<=(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmple_epi64_mask(a, b));
}

inline Vec8Mask operator>=(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpge_epi64_mask(a, b));
}

inline Vec8Mask operator!=(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpneq_epi64_mask(a, b));
}

// ---------------------------------------------------------
// Vec8 Arithmetic Operations
// ---------------------------------------------------------

inline Vec8 operator+(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_add_epi64(a, b)); }

inline Vec8 &operator+=(Vec8 &a, const Vec8 &b) {
  a = a + b;
  return a;
}

inline Vec8 operator-(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_sub_epi64(a, b)); }

inline Vec8 &operator-=(Vec8 &a, const Vec8 &b) {
  a = a - b;
  return a;
}

inline Vec8 operator*(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_mullo_epi64(a, b)); }

inline Vec8 &operator*=(Vec8 &a, const Vec8 &b) {
  a = a * b;
  return a;
}

inline Vec8 operator&(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_and_epi64(a, b)); }

inline Vec8 &operator&=(Vec8 &a, const Vec8 &b) {
  a = a & b;
  return a;
}

inline Vec8 operator|(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_or_epi64(a, b)); }

inline Vec8 &operator|=(Vec8 &a, const Vec8 &b) {
  a = a | b;
  return a;
}

inline Vec8 operator^(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_xor_epi64(a, b)); }

inline Vec8 &operator^=(Vec8 &a, const Vec8 &b) {
  a = a ^ b;
  return a;
}

inline Vec8 operator>>(const Vec8 &a, const uint32_t shift) {
  return Vec8(_mm512_srli_epi64(a, shift));
}

inline Vec8 &operator>>=(Vec8 &a, const uint32_t shift) {
  a = a >> shift;
  return a;
}

inline Vec8 operator<<(const Vec8 &a, const uint32_t shift) {
  return Vec8(_mm512_slli_epi64(a, shift));
}

inline Vec8 &operator<<=(Vec8 &a, const uint32_t shift) {
  a = a << shift;
  return a;
}

inline Vec8 operator>>(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_srlv_epi64(a, b)); }

inline Vec8 &operator>>=(Vec8 &a, const Vec8 &b) {
  a = a >> b;
  return a;
}

inline Vec8 operator<<(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_sllv_epi64(a, b)); }

inline Vec8 &operator<<=(Vec8 &a, const Vec8 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Vec16 Comparison Operations
// ---------------------------------------------------------

inline Vec16Mask operator>(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpgt_epi32_mask(a, b));
}

inline Vec16Mask operator==(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpeq_epi32_mask(a, b));
}

inline Vec16Mask operator<(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmplt_epi32_mask(a, b));
}

inline Vec16Mask operator<=(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmple_epi32_mask(a, b));
}

inline Vec16Mask operator>=(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpge_epi32_mask(a, b));
}

inline Vec16Mask operator!=(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpneq_epi32_mask(a, b));
}

// ---------------------------------------------------------
// Vec16 Arithmetic Operations
// ---------------------------------------------------------

inline Vec16 operator+(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_add_epi32(a, b)); }

inline Vec16 &operator+=(Vec16 &a, const Vec16 &b) {
  a = a + b;
  return a;
}

inline Vec16 operator-(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_sub_epi32(a, b)); }

inline Vec16 &operator-=(Vec16 &a, const Vec16 &b) {
  a = a - b;
  return a;
}

inline Vec16 operator&(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_and_epi32(a, b)); }

inline Vec16 &operator&=(Vec16 &a, const Vec16 &b) {
  a = a & b;
  return a;
}

inline Vec16 operator|(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_or_epi32(a, b)); }

inline Vec16 &operator|=(Vec16 &a, const Vec16 &b) {
  a = a | b;
  return a;
}

inline Vec16 operator^(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_xor_epi32(a, b)); }

inline Vec16 &operator^=(Vec16 &a, const Vec16 &b) {
  a = a ^ b;
  return a;
}

inline Vec16 operator>>(const Vec16 &a, const uint32_t shift) {
  return Vec16(_mm512_srli_epi32(a, shift));
}

inline Vec16 &operator>>=(Vec16 &a, const uint32_t shift) {
  a = a >> shift;
  return a;
}

inline Vec16 operator<<(const Vec16 &a, const uint32_t shift) {
  return Vec16(_mm512_slli_epi32(a, shift));
}

inline Vec16 &operator<<=(Vec16 &a, const uint32_t shift) {
  a = a << shift;
  return a;
}

inline Vec16 operator>>(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_srlv_epi32(a, b)); }

inline Vec16 &operator>>=(Vec16 &a, const Vec16 &b) {
  a = a >> b;
  return a;
}

inline Vec16 operator<<(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_sllv_epi32(a, b)); }

inline Vec16 &operator<<=(Vec16 &a, const Vec16 &b) {
  a = a << b;
  return a;
}

}  // namespace tpl::util::simd
