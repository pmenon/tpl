#include "sql/operations/like_operators.h"

#include "common/macros.h"

namespace tpl::sql {

// Inspired by Postgres
bool Like::Apply(const char *str, const char *pattern, const char escape) {
  TPL_ASSERT(str != nullptr, "Input string cannot be NULL");
  TPL_ASSERT(pattern != nullptr, "Pattern cannot be NULL");

  const char *s = str, *p = pattern;
  for (; *p != 0 && *s != 0; p++) {
    if (*p == escape) {
      // Next pattern character must match exactly, whatever it is
      p++;

      if (*p != *s) {
        return false;
      }

      s++;
    } else if (*p == '%') {
      // Any sequence of '%' wildcards can essentially be replaced by one '%'. Similarly, any
      // sequence of N '_'s will blindly consume N characters from the input string. Process the
      // pattern until we reach a non-wildcard character.
      p++;
      while (*p != 0) {
        if (*p == '%') {
          p++;
        } else if (*p == '_') {
          if (*s == 0) {
            return false;
          }
          s++;
          p++;
        } else {
          break;
        }
      }

      // If we've reached the end of the pattern, the tail of the input string is accepted.
      if (*p == 0) {
        return true;
      }

      if (*p == escape) {
        p++;
        TPL_ASSERT(*p != 0, "LIKE pattern must not end with an escape character");
        if (*p == 0) {
          return false;
        }
      }

      while (*s != 0) {
        if (Like::Apply(s, p, escape)) {
          return true;
        }
        s++;
      }
      // No match
      return false;
    } else if (*p == '_') {
      // '_' wildcard matches a single character in the input
      s++;
    } else if (*p == *s) {
      // Exact character match
      s++;
    } else {
      // Unmatched!
      return false;
    }
  }

  return *s == 0 && *p == 0;
}

}  // namespace tpl::sql
