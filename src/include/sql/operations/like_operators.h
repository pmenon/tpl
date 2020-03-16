#pragma once

#include <cstdlib>

#include "sql/runtime_types.h"

namespace tpl::sql {

static constexpr const char kDefaultEscape = '\\';

/**
 * Functor implementing the SQL LIKE() operator
 */
struct Like {
  static bool Apply(const char *str, std::size_t str_len, const char *pattern,
                    std::size_t pattern_len, char escape = kDefaultEscape);

  bool operator()(const VarlenEntry &str, const VarlenEntry &pattern,
                  char escape = kDefaultEscape) const {
    return Apply(reinterpret_cast<const char *>(str.GetContent()), str.GetSize(),
                 reinterpret_cast<const char *>(pattern.GetContent()), pattern.GetSize(), escape);
  }
};

/**
 * Functor implementing the SQL NOT LIKE() operator
 */
struct NotLike {
  static bool Apply(const char *str, std::size_t str_len, const char *pattern,
                    std::size_t pattern_len, char escape = kDefaultEscape) {
    return !Like::Apply(str, str_len, pattern, pattern_len, escape);
  }

  bool operator()(const VarlenEntry &str, const VarlenEntry &pattern,
                  char escape = kDefaultEscape) const {
    return !Like::Apply(reinterpret_cast<const char *>(str.GetContent()), str.GetSize(),
                        reinterpret_cast<const char *>(pattern.GetContent()), pattern.GetSize(),
                        escape);
  }
};

}  // namespace tpl::sql
