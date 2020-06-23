#pragma once

#include <x86intrin.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "xxh3.h"  // NOLINT

#include "common/common.h"
#include "common/macros.h"

namespace tpl::util {

/**
 * Generic hashing utility class.
 */
class HashUtil : public AllStatic {
 public:
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

  /**
   * Compute a new hash value that scrambles the bits in the input hash value. This function
   * guarantees that if h1 and h2 are two hash values, then scramble(h1) == scramble(h2).
   * @param hash The input hash value to scramble.
   * @return The scrambled hash value.
   */
  static hash_t ScrambleHash(const hash_t hash) { return XXH64_avalanche(hash); }

  // -------------------------------------------------------
  // CRC32 Hashing
  // -------------------------------------------------------

  /**
   * Compute the CRC32 hash of the input value @em val with the provided seed hash. This function
   * uses an SSE 4.2 optimized hardware implementation of CRC32 and only works on native CPP types.
   * @tparam T The type of value. Must be a fundamental type, i.e., bool, integral, or
   *           floating-point number type.
   * @param val The value to hash.
   * @param seed The seed hash value.
   * @return The CRC32 hash of the input.
   */
  template <typename T>
  static auto HashCrc(T val, hash_t seed) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    // Thanks HyPer
    static constexpr hash_t kDefaultCRCSeed = 0x04c11db7ULL;

    uint64_t result1 = _mm_crc32_u64(seed, static_cast<uint64_t>(val));
    uint64_t result2 = _mm_crc32_u64(kDefaultCRCSeed, static_cast<uint64_t>(val));
    return ((result2 << 32u) | result1) * 0x2545f4914f6cdd1dULL;
  }

  /**
   * Compute the CRC32 hash of the input value @em val. The input must be a fundamental type,
   * i.e., bool, integral, or floating-point number type. This function uses an SSE 4.2 optimized
   * hardware implementation of CRC32 and only works on native CPP types.
   * @tparam T The type of value to hash.
   * @param val The value to hash.
   * @return The CRC32 hash of the input.
   */
  template <typename T>
  static auto HashCrc(T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashCrc(val, 0);
  }

  /**
   * Compute the CRC32 hash of the buffer pointed to by @em buf with the provided length and seed
   * value. This function uses an SSE 4.2 optimized hardware implementation of CRC32.
   * @param buf The buffer to hash.
   * @param len The length of the buffer.
   * @param seed The seed value.
   * @return The CRC32 hash of the input buffer.
   */
  static hash_t HashCrc(const uint8_t *buf, uint32_t len, hash_t seed) {
    uint64_t hash = seed;

    // Process as many 8-byte chunks as possible.
    for (; len >= 8; buf += 8, len -= 8) {
      hash = HashCrc(*reinterpret_cast<const uint64_t *>(buf), hash);
    }

    // If there's at least a 4-byte chunk, process that.
    if (len >= 4) {
      hash = HashCrc(*reinterpret_cast<const uint32_t *>(buf), hash);
      buf += 4;
      len -= 4;
    }

    // Process the tail.
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

  /**
   * Compute the CRC32 hash of the buffer pointed to by @em buf with the provided length. This
   * function uses an SSE 4.2 optimized hardware implementation of CRC32.
   * @param buf The buffer to hash.
   * @param len The length of the buffer.
   * @param seed The seed value.
   * @return The CRC32 hash of the input buffer.
   */
  static hash_t HashCrc(const uint8_t *buf, uint32_t len) { return HashCrc(buf, len, 0); }

  // -------------------------------------------------------
  // Murmur3 Hashing
  // -------------------------------------------------------

  /**
   * Compute the Murmur3 hash of the provided input value @em val using the provided seed hash. This
   * is just the finalizer of the Murmur3 algorithm.
   * @tparam T The type of value. Must be a fundamental type, i.e., bool, integral, or
   *           floating-point number type.
   * @param val The value to hash.
   * @param seed The seed hash.
   * @return The Murmur3 hash of the input.
   */
  template <typename T>
  static auto HashMurmur(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    auto k = static_cast<uint64_t>(val);
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdLLU;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53LLU;
    k ^= k >> 33;
    return k ^ seed;
  }

  /**
   * Compute the Murmur3 hash of the provided input value @em val. This is just the finalizer of the
   * Murmur3 algorithm.
   * @tparam T The type of value. Must be a fundamental type, i.e., bool, integral, or
   *           floating-point number type.
   * @param val The value to hash.
   * @return The Murmur3 hash of the input.
   */
  template <typename T>
  static auto HashMurmur(T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashMurmur(val, 0);
  }

  // -------------------------------------------------------
  // XXH3 Hashing
  // -------------------------------------------------------

  /**
   * Compute the XXH3 hash of the input buffer @em buf with length @em length using the provided
   * seed hash @em seed.
   * @param buf The buffer to hash.
   * @param len The length of the buffer.
   * @param seed The seed hash.
   * @return The XXH3 hash of the input.
   */
  static hash_t HashXXH3(const uint8_t *buf, uint32_t len, hash_t seed) {
    return XXH3_64bits_withSeed(buf, len, seed);
  }

  /**
   * Compute the XXH3 hash of the input buffer @em buf with length @em length.
   * @param buf The buffer to hash.
   * @param len The length of the buffer.
   * @return The XXH3 hash of the input.
   */
  static hash_t HashXXH3(const uint8_t *buf, uint32_t len) { return XXH3_64bits(buf, len); }

  /**
   * Compute the XXH3 hash of the input string @em s.
   * @param s The string to hash.
   * @return The XXH3 hash of the input.
   */
  static hash_t HashXXH3(std::string_view s) { return XXH3_64bits(s.data(), s.length()); }

  /**
   * Compute the XXH3 hash of the input value @em val using the provided seed hash @em seed.
   * @tparam T The type of the input to hash. Must be a fundamental type, i.e., bool, integral, or
   *           floating-point number type.
   * @param val The value to hash.
   * @param seed The seed hash.
   * @return The XXH3 hash of the input.
   */
  template <typename T>
  static auto HashXXH3(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return XXH3_64bits_withSeed(&val, sizeof(T), seed);
  }

  /**
   * Compute the XXH3 hash of the input value @em val.
   * @tparam T The type of the input to hash. Must be a fundamental type, i.e., bool, integral, or
   *           floating-point number type.
   * @param val The value to hash.
   * @return The XXH3 hash of the input.
   */
  template <typename T>
  static auto HashXXH3(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return XXH3_64bits(&val, sizeof(T));
  }
};

}  // namespace tpl::util
