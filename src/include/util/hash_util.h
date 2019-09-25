#pragma once

#include <x86intrin.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "xxh3.h"

#include "common/common.h"
#include "common/macros.h"

namespace tpl::util {

/**
 * Enumeration of the supported hashing methods
 */
enum class HashMethod : uint8_t { Crc, Murmur2, xxHash3 };

/**
 * Generic hashing utility class. The main entry points into this utility class are the Hash()
 * functions. There are specialized versions for arithmetic values (integers and floats), and
 * generic versions for longer buffers (strings, c-strings, and opaque buffers).
 */
class HashUtil {
 public:
  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be either an integral
   * numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam METHOD The hash method to use.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @return The compute hash.
   */
  template <HashMethod METHOD = HashMethod::Murmur2, typename T>
  static auto Hash(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    switch (METHOD) {
      case HashMethod::Crc:
        return HashCrc(val, seed);
      case HashMethod::Murmur2:
        return HashMurmur2(val, seed);
      case HashMethod::xxHash3:
        return HashXX3(val, seed);
    }
  }

  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be either an integral
   * numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam METHOD The hash method to use.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @param seed The seed hash value to mix in.
   * @return The compute hash.
   */
  template <HashMethod METHOD = HashMethod::Murmur2, typename T>
  static auto Hash(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    switch (METHOD) {
      case HashMethod::Crc:
        return HashCrc(val);
      case HashMethod::Murmur2:
        return HashMurmur2(val);
      case HashMethod::xxHash3:
        return HashXX3(val);
    }
  }

  /**
   * Compute the hash value of an input buffer @em buf with length @em len.
   * @tparam METHOD The hash method to use.
   * @param buf The input buffer.
   * @param len The length of the input buffer to hash.
   * @return The computed hash value based on the contents of the input buffer.
   */
  template <HashMethod METHOD = HashMethod::xxHash3>
  static hash_t Hash(const uint8_t *buf, uint32_t len) {
    switch (METHOD) {
      case HashMethod::Crc:
        return HashCrc(buf, len);
      case HashMethod::Murmur2:
        return HashMurmur2(buf, len);
      case HashMethod::xxHash3:
        return HashXX3(buf, len);
    }
  }

  /**
   * Compute the hash value of an input buffer @em buf with length @em len.
   * @tparam METHOD The hash method to use.
   * @param buf The input buffer.
   * @param len The length of the input buffer to hash.
   * @param seed The seed hash value to mix in.
   * @return The computed hash value based on the contents of the input buffer.
   */
  template <HashMethod METHOD = HashMethod::xxHash3>
  static hash_t Hash(const uint8_t *buf, uint32_t len, const hash_t seed) {
    switch (METHOD) {
      case HashMethod::Crc:
        return HashCrc(buf, len, seed);
      case HashMethod::Murmur2:
        return HashMurmur2(buf, len, seed);
      case HashMethod::xxHash3:
        return HashXX3(buf, len, seed);
    }
  }

  /**
   * Compute the hash value of an input string view @em s.
   * @tparam METHOD The hash method to use.
   * @param s The input string.
   * @return The computed hash value based on the contents of the input string.
   */
  template <HashMethod METHOD = HashMethod::xxHash3>
  static hash_t Hash(const std::string_view s) {
    return Hash<METHOD>(reinterpret_cast<const uint8_t *>(s.data()), s.length());
  }

  /**
   * Combine and mix two hash values into a new hash value
   * @param first_hash The first hash value
   * @param second_hash The second hash value
   * @return The mixed hash value
   */
  static hash_t CombineHashes(const hash_t first_hash, const hash_t second_hash) {
    // Based on Hash128to64() from cityhash.
    static constexpr uint64_t kMul = uint64_t(0x9ddfea08eb382d69);
    hash_t a = (first_hash ^ second_hash) * kMul;
    a ^= (a >> 47u);
    hash_t b = (second_hash ^ a) * kMul;
    b ^= (b >> 47u);
    b *= kMul;
    return b;
  }

  // -------------------------------------------------------
  // CRC Hashing
  // -------------------------------------------------------

  static constexpr hash_t kDefaultCRCSeed = 0x04C11DB7;

#define DO_CRC(op, crc, type, buf, len)                                           \
  do {                                                                            \
    for (; (len) >= sizeof(type); (len) -= sizeof(type), (buf) += sizeof(type)) { \
      (crc) = op((crc), *(type *)buf);                                            \
    }                                                                             \
  } while (0)

  static hash_t HashCrc(const uint8_t *buf, uint32_t len, hash_t seed) {
    hash_t hash_val = seed;

    // If the string is empty, return the CRC calculated so far
    if (len == 0) {
      return hash_val;
    }

    // Try to consume chunks of 8-byte, 4-byte, 2-byte, and 1-bytes.
    DO_CRC(_mm_crc32_u64, hash_val, uint64_t, buf, len);
    DO_CRC(_mm_crc32_u32, hash_val, uint32_t, buf, len);
    DO_CRC(_mm_crc32_u16, hash_val, uint16_t, buf, len);
    DO_CRC(_mm_crc32_u8, hash_val, uint8_t, buf, len);

    return hash_val;
  }

  static hash_t HashCrc(const uint8_t *buf, uint32_t len) {
    return HashCrc(buf, len, kDefaultCRCSeed);
  }

  template <typename T>
  static auto HashCrc(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashCrc(reinterpret_cast<const uint8_t *>(&val), sizeof(T), seed);
  }

  template <typename T>
  static auto HashCrc(const T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashCrc(val, kDefaultCRCSeed);
  }

#undef DO_CRC

  // -------------------------------------------------------
  // Murmur2 Hashing
  // -------------------------------------------------------

  // MurmurHash2, 64-bit versions, by Austin Appleby
  // https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
  // 'kMurmur2Prime' and 'kMurmur2R' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.
  static constexpr uint64_t kMurmur2Prime = 0xc6a4a7935bd1e995;
  static constexpr int32_t kMurmur2R = 47;

  template <typename T>
  static auto HashMurmur2(const T val, hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    // MurmurHash64A
    uint64_t k = static_cast<uint64_t>(val);
    hash_t h = seed ^ 0x8445d61a4e774912 ^ (8 * kMurmur2Prime);
    k *= kMurmur2Prime;
    k ^= k >> kMurmur2R;
    k *= kMurmur2Prime;
    h ^= k;
    h *= kMurmur2Prime;
    h ^= h >> kMurmur2R;
    h *= kMurmur2Prime;
    h ^= h >> kMurmur2R;
    return h;
  }

  template <typename T>
  static hash_t HashMurmur2(const T k) {
    return HashMurmur2(k, 0);
  }

  static hash_t HashMurmur2(const uint8_t *buf, uint32_t len, hash_t seed) {
    // MurmurHash64A
    hash_t h = seed ^ (len * kMurmur2Prime);

    const uint64_t *data = reinterpret_cast<const uint64_t *>(buf);
    const uint64_t *end = data + (len / 8);

    while (data != end) {
      uint64_t k = *data++;

      k *= kMurmur2Prime;
      k ^= k >> kMurmur2R;
      k *= kMurmur2Prime;

      h ^= k;
      h *= kMurmur2Prime;
    }

    const uint8_t *data2 = reinterpret_cast<const uint8_t *>(data);

    switch (len & 7) {
      case 7:
        h ^= uint64_t(data2[6]) << 48;
        FALLTHROUGH;
      case 6:
        h ^= uint64_t(data2[5]) << 40;
        FALLTHROUGH;
      case 5:
        h ^= uint64_t(data2[4]) << 32;
        FALLTHROUGH;
      case 4:
        h ^= uint64_t(data2[3]) << 24;
        FALLTHROUGH;
      case 3:
        h ^= uint64_t(data2[2]) << 16;
        FALLTHROUGH;
      case 2:
        h ^= uint64_t(data2[1]) << 8;
        FALLTHROUGH;
      case 1:
        h ^= uint64_t(data2[0]);
        h *= kMurmur2Prime;
    }

    h ^= h >> kMurmur2R;
    h *= kMurmur2Prime;
    h ^= h >> kMurmur2R;

    return h;
  }

  static hash_t HashMurmur2(const uint8_t *buf, uint32_t len) { return HashMurmur2(buf, len, 0); }

  // -------------------------------------------------------
  // XXH3 Hashing
  // -------------------------------------------------------

  static hash_t HashXX3(const uint8_t *buf, uint32_t len, hash_t seed) {
    return XXH3_64bits_withSeed(buf, len, seed);
  }

  static hash_t HashXX3(const uint8_t *buf, uint32_t len) { return XXH3_64bits(buf, len); }

  template <typename T>
  static auto HashXX3(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashXX3(reinterpret_cast<const uint8_t *>(&val), sizeof(T), seed);
  }

  template <typename T>
  static auto HashXX3(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashXX3(reinterpret_cast<const uint8_t *>(&val), sizeof(T));
  }
};

}  // namespace tpl::util
