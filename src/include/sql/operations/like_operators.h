#pragma once

#include <cstdlib>

#include "sql/runtime_types.h"

namespace tpl::sql {

static constexpr const char kDefaultEscape = '\\';

/**
 * Functor implementing the SQL LIKE() operator
 */
struct Like {
  static bool Impl(const char *str, std::size_t str_len, const char *pattern,
                   std::size_t pattern_len, char escape = kDefaultEscape);

  bool operator()(const VarlenEntry &str, const VarlenEntry &pattern,
                  char escape = kDefaultEscape) const {
    return Impl(reinterpret_cast<const char *>(str.GetContent()), str.GetSize(),
                reinterpret_cast<const char *>(pattern.GetContent()), pattern.GetSize(), escape);
  }
};

/**
 * Functor implementing the SQL NOT LIKE() operator
 */
struct NotLike {
  bool operator()(const VarlenEntry &str, const VarlenEntry &pattern,
                  char escape = kDefaultEscape) const {
    return !Like{}(str, pattern, escape);
  }
};

}  // namespace tpl::sql
