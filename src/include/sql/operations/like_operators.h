#pragma once

namespace tpl::sql {

constexpr static const char kDefaultEscape = '\\';

/**
 * Functor implementing the SQL LIKE() operator
 */
struct Like {
  static bool Apply(const char *str, const char *pattern, char escape = kDefaultEscape);
};

/**
 * Functor implementing the SQL NOT LIKE() operator
 */
struct NotLike {
  static bool Apply(const char *str, const char *pattern, const char escape = kDefaultEscape) {
    return !Like::Apply(str, pattern, escape);
  }
};

}  // namespace tpl::sql
