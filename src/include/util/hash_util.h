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
 * Generic hashing utility class. The main entry point are the HashUtil::Hash() functions. There are
 * overloaded specialized versions for arithmetic values (integers and floats), and generic versions
 * for longer buffers (strings, c-strings, and opaque buffers).
 */
class HashUtil : public AllStatic {
 public:
  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be either an integral
   * numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @param seed The seed hash value to mix in.
   * @return The computed hash.
   */
  template <typename T>
  static auto Hash(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashMurmur(val, seed);
  }

  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be either an integral
   * numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @return The computed hash.
   */
  template <typename T>
  static auto Hash(T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashMurmur(val);
  }

  /**
   * Compute the hash value of the input buffer with the provided length.
   * @param buf The input buffer.
   * @param len The length of the input buffer to hash.
   * @return The computed hash value based on the contents of the input buffer.
   */
  static auto Hash(const uint8_t *buf, std::size_t len) -> hash_t { return HashXX3(buf, len); }

  /**
   * Compute the hash value of the input buffer with the provided length and using a seed hash.
   * @param buf The input buffer.
   * @param len The length of the input buffer to hash.
   * @param seed The seed hash value to mix in.
   * @return The computed hash value based on the contents of the input buffer.
   */
  static auto Hash(const uint8_t *buf, std::size_t len, hash_t seed) -> hash_t {
    return HashXX3(buf, len, seed);
  }

  /**
   * Compute the hash value of an input string view @em s.
   * @param s The input string.
   * @return The computed hash value based on the contents of the input string.
   */
  static auto Hash(const std::string_view s) -> hash_t {
    return HashXX3(reinterpret_cast<const uint8_t *>(s.data()), s.length());
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

  /**
   * Compute a new hash value that scrambles the bits in the input hash value. This function
   * guarantees that if h1 and h2 are two hash values, then scramble(h1) == scramble(h2).
   * @param hash The input hash value to scramble.
   * @return The scrambled hash value.
   */
  static hash_t ScrambleHash(const hash_t hash) { return XXH64_avalanche(hash); }

  // -------------------------------------------------------
  // CRC Hashing - Integers Only
  // -------------------------------------------------------

  template <typename T>
  static auto HashCrc(T val, hash_t seed) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    // Thanks HyPer
    static constexpr hash_t kDefaultCRCSeed = 0x04c11db7ULL;

    uint64_t result1 = _mm_crc32_u64(seed, static_cast<uint64_t>(val));
    uint64_t result2 = _mm_crc32_u64(kDefaultCRCSeed, static_cast<uint64_t>(val));
    return ((result2 << 32u) | result1) * 0x2545f4914f6cdd1dULL;
  }

  template <typename T>
  static auto HashCrc(T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashCrc(val, 0);
  }

  // -------------------------------------------------------
  // Murmur3 Hashing - Integers Only
  // -------------------------------------------------------

  template <typename T>
  static auto HashMurmur(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    auto k = static_cast<uint64_t>(val);
    k ^= seed;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdLLU;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53LLU;
    k ^= k >> 33;
    return k;
  }

  template <typename T>
  static auto HashMurmur(T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashMurmur(val, 0);
  }

  // -------------------------------------------------------
  // XXH3 Hashing - All Types
  // -------------------------------------------------------

  static hash_t HashXX3(const uint8_t *buf, uint32_t len, hash_t seed) {
    return XXH3_64bits_withSeed(buf, len, seed);
  }

  static hash_t HashXX3(const uint8_t *buf, uint32_t len) { return XXH3_64bits(buf, len); }

  template <typename T>
  static auto HashXX3(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return XXH3_64bits_withSeed(&val, sizeof(T), seed);
  }

  template <typename T>
  static auto HashXX3(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return XXH3_64bits(&val, sizeof(T));
  }
};

}  // namespace tpl::util
