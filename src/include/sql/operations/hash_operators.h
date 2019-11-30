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
 * Primitive hashing.
 */
#define DECL_HASH(Type, ...)                                       \
  template <>                                                      \
  struct Hash<Type> {                                              \
    static hash_t Apply(Type input, bool null) {                   \
      return null ? hash_t(0) : util::HashUtil::HashMurmur(input); \
    }                                                              \
  };

ALL_TYPES(DECL_HASH)
#undef DECL_HASH

/**
 * Date hashing.
 */
template <>
struct Hash<Date> {
  static hash_t Apply(Date input, bool null) { return null ? hash_t(0) : input.Hash(); }
};

/**
 * Timestamp hashing.
 */
template <>
struct Hash<Timestamp> {
  static hash_t Apply(Timestamp input, bool null) { return null ? hash_t(0) : input.Hash(); }
};

/**
 * String hashing.
 */
template <>
struct Hash<VarlenEntry> {
  static hash_t Apply(const VarlenEntry &input, bool null) {
    return null ? hash_t(0) : input.Hash();
  }
};

}  // namespace tpl::sql
