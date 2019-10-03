#pragma once

#include "util/hash_util.h"

namespace tpl::sql {

/**
 * Hash operation functor. Dispatches to tpl::util::Hash() for non-null inputs.
 */
struct Hash {
  template <typename T>
  static auto Apply(T input, bool null) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return null ? hash_t(0) : util::HashUtil::HashCrc(input);
  }

  static hash_t Apply(const Date &input, bool null) { return null ? hash_t(0) : input.Hash(); }

  static hash_t Apply(const VarlenEntry &input, bool null) {
    return null ? hash_t(0) : input.Hash();
  }
};

}  // namespace tpl::sql
