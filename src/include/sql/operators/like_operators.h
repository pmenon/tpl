#pragma once

#include <cstdlib>

#include "sql/runtime_types.h"

namespace tpl::sql {

/**
 * Functor implementing the SQL LIKE() operator.
 */
struct Like {
  static constexpr const char kDefaultEscape = '\\';

  bool operator()(const VarlenEntry &str, const VarlenEntry &pattern,
                  char escape = kDefaultEscape) const;
};

/**
 * Functor implementing the SQL NOT LIKE() operator.
 */
struct NotLike {
  bool operator()(const VarlenEntry &str, const VarlenEntry &pattern,
                  char escape = Like::kDefaultEscape) const {
    return !Like{}(str, pattern, escape);
  }
};

}  // namespace tpl::sql
