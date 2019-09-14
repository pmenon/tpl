#pragma once

#include <x86intrin.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "common/common.h"
#include "common/macros.h"

namespace tpl::util {

/**
 * Enumeration of the supported hashing methods
 */
enum class HashMethod : uint8_t { Fnv1, Crc, Murmur2, xxHash3 };

/**
 * Generic hashing utility class. The main entry points into this utility class
 * are the 'Hash' functions. There are specialized, i.e., templated, versions
 * for arithmetic values (integers and floats), and generic versions for longer
 * buffers (strings, c-strings, and opaque buffers).
 */
class Hasher {
 public:
  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be
   * either an integral numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam METHOD The hash method to use.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @return The compute hash.
   */
  template <HashMethod METHOD = HashMethod::Crc, typename T>
  static auto Hash(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    switch (METHOD) {
      case HashMethod::Fnv1:
        return HashFnv1(val, seed);
      case HashMethod::Crc:
        return HashCrc(val, seed);
      case HashMethod::Murmur2:
        return HashMurmur2(val, seed);
      case HashMethod::xxHash3:
        return HashXX3(val, seed);
    }
  }

  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be
   * either an integral numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam METHOD The hash method to use.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @param seed The seed hash value to mix in.
   * @return The compute hash.
   */
  template <HashMethod METHOD = HashMethod::Crc, typename T>
  static auto Hash(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    switch (METHOD) {
      case HashMethod::Fnv1:
        return HashFnv1(val);
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
      case HashMethod::Fnv1:
        return HashFnv1(buf, len);
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
      case HashMethod::Fnv1:
        return HashFnv1(buf, len, seed);
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

  template <typename T>
  static auto HashCrc(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    uint64_t result1 = _mm_crc32_u64(seed, static_cast<uint64_t>(val));
    uint64_t result2 = _mm_crc32_u64(0x04C11DB7, static_cast<uint64_t>(val));
    return ((result2 << 32u) | result1) * 0x2545F4914F6CDD1Dull;
  }

  template <typename T>
  static auto HashCrc(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashCrc(val, hash_t{0});
  }

  static hash_t HashCrc(const uint8_t *buf, uint32_t len, hash_t seed) {
    // Thanks HyPer
    uint64_t hash = seed;

    // Process as many 8-byte chunks as possible
    for (; len >= 8; buf += 8, len -= 8) {
      hash = HashCrc(*reinterpret_cast<const uint64_t *>(buf), hash);
    }

    // If there's at least a 4-byte chunk, process that
    if (len >= 4) {
      hash = HashCrc(*reinterpret_cast<const uint32_t *>(buf), hash);
      buf += 4;
      len -= 4;
    }

    // Process the tail
    switch (len) {
      case 3:
        hash ^= (static_cast<uint64_t>(buf[2])) << 16u;
        FALLTHROUGH;
      case 2:
        hash ^= (static_cast<uint64_t>(buf[1])) << 8u;
        FALLTHROUGH;
      case 1:
        hash ^= buf[0];
        FALLTHROUGH;
      default:
        break;
    }

    return hash;
  }

  static hash_t HashCrc(const uint8_t *buf, uint32_t len) { return HashCrc(buf, len, 0); }

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
  // xx3 Hashing
  // -------------------------------------------------------

  static hash_t HashXX3(const uint8_t *buf, uint32_t len, hash_t seed);

  static hash_t HashXX3(const uint8_t *buf, uint32_t len) { return HashXX3(buf, len, 0); }

  template <typename T>
  static auto HashXX3(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashXX3(reinterpret_cast<const uint8_t *>(&val), sizeof(T), seed);
  }

  template <typename T>
  static auto HashXX3(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashXX3(reinterpret_cast<const uint8_t *>(&val), sizeof(T));
  }

  // -------------------------------------------------------
  // FNV
  // -------------------------------------------------------

  // default values recommended by http://isthe.com/chongo/tech/comp/fnv/
  static constexpr uint64_t kFNV64Prime = 1099511628211UL;
  static constexpr uint64_t kFNV64Seed = 14695981039346656037UL;

  static hash_t HashFnv1(const uint8_t *buf, uint32_t len, hash_t seed) {
    while (len--) {
      seed = (*buf ^ seed) * kFNV64Prime;
      ++buf;
    }
    return seed;
  }

  static hash_t HashFnv1(const uint8_t *buf, uint32_t len) {
    return HashFnv1(buf, len, kFNV64Seed);
  }

  template <typename T>
  static auto HashFnv1(const T val, const hash_t seed)
      -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashFnv1(reinterpret_cast<const uint8_t *>(&val), sizeof(T), seed);
  }

  template <typename T>
  static auto HashFnv1(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashFnv1(reinterpret_cast<const uint8_t *>(&val), sizeof(T));
  }
};

}  // namespace tpl::util
