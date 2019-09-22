#pragma once

#include "util/hash_util.h"

namespace tpl::sql {

/**
 * Hash operation functor. Dispatches to tpl::util::Hash() for non-null inputs.
 */
struct Hash {
  template <typename T>
  static hash_t Apply(T input, bool null) {
    return null ? hash_t(0) : util::HashUtil::Hash(input);
  }

  static hash_t Apply(const Date &input, bool null) { return null ? hash_t(0) : input.Hash(); }
};

}  // namespace tpl::sql
