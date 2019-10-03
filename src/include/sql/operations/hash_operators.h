#pragma once

#include "util/hash_util.h"

namespace tpl::sql {

/**
 * Hash operation functor. Dispatches to tpl::util::Hash() for non-null inputs.
 */
template <typename T>
struct Hash {
  static auto Apply(T input, bool null) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return null ? hash_t(0) : util::HashUtil::HashCrc(input);
  }
};

/**
 * Hash for Date.
 */
template <>
struct Hash<Date> {
  static hash_t Apply(const Date &input, bool null) { return null ? hash_t(0) : input.Hash(); }
};

/**
 * Hash for Timestamp.
 */
template <>
struct Hash<Timestamp> {
  static hash_t Apply(const Timestamp &input, bool null) { return null ? hash_t(0) : input.Hash(); }
};

/**
 * Hash for VarlenEntrys.
 */
template <>
struct Hash<VarlenEntry> {
  static hash_t Apply(const VarlenEntry &input, bool null) {
    return null ? hash_t(0) : input.Hash();
  }
};

}  // namespace tpl::sql
