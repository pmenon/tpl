#pragma once

#include "common/common.h"
#include "util/hash_util.h"

namespace tpl::sql {

/**
 * Hash operation functor.
 */
template <typename T>
struct Hash;

/**
 * Hash-with-seed functor.
 */
template <typename T>
struct HashCombine;

/**
 * Primitive hashing.
 */
#define DECL_HASH(Type, ...)                                                     \
  template <>                                                                    \
  struct Hash<Type> {                                                            \
    hash_t operator()(Type input, bool null) const noexcept {                    \
      return null ? hash_t(0) : util::HashUtil::HashCrc(input);                  \
    }                                                                            \
  };                                                                             \
  template <>                                                                    \
  struct HashCombine<Type> {                                                     \
    hash_t operator()(Type input, bool null, const hash_t seed) const noexcept { \
      return null ? hash_t(0) : util::HashUtil::HashCrc(input, seed);            \
    }                                                                            \
  };

BOOL_TYPES(DECL_HASH)
INT_TYPES(DECL_HASH)
FLOAT_TYPES(DECL_HASH)
#undef DECL_HASH

/**
 * Date hashing.
 */
template <>
struct Hash<Date> {
  hash_t operator()(Date input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Dating hashing with seed.
 */
template <>
struct HashCombine<Date> {
  hash_t operator()(Date input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

/**
 * Timestamp hashing.
 */
template <>
struct Hash<Timestamp> {
  hash_t operator()(Timestamp input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Timestamp hashing with seed.
 */
template <>
struct HashCombine<Timestamp> {
  hash_t operator()(Timestamp input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

/**
 * String hashing.
 */
template <>
struct Hash<VarlenEntry> {
  hash_t operator()(const VarlenEntry &input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Varlen hashing with seed.
 */
template <>
struct HashCombine<VarlenEntry> {
  hash_t operator()(const VarlenEntry &input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

}  // namespace tpl::sql
