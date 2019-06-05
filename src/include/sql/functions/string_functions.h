#pragma once

#include <limits>

#include "sql/value.h"

namespace tpl::sql {

class ExecutionContext;

/**
 * Utility class to handle SQL string (i.e., varchar, char, etc.) manipulations.
 */
class StringFunctions {
 public:
  // Delete to force only static functions
  StringFunctions() = delete;

  static void Substring(ExecutionContext *ctx, StringVal *result,
                        const StringVal &str, const Integer &pos,
                        const Integer &len);

  static void Substring(ExecutionContext *ctx, StringVal *result,
                        const StringVal &str, const Integer &pos) {
    Substring(ctx, result, str, pos, Integer(std::numeric_limits<i64>::max()));
  }

  static void SplitPart(ExecutionContext *ctx, StringVal *result,
                        const StringVal &str, const StringVal &delim,
                        const Integer &field);

  static void Repeat(ExecutionContext *ctx, StringVal *result,
                     const StringVal &str, const Integer &n);

  static void Lpad(ExecutionContext *ctx, StringVal *result,
                   const StringVal &str, const Integer &len,
                   const StringVal &pad);

  static void Rpad(ExecutionContext *ctx, StringVal *result,
                   const StringVal &str, const Integer &, const StringVal &pad);

  static void Length(ExecutionContext *ctx, Integer *result,
                     const StringVal &str);

  static void CharLength(ExecutionContext *ctx, Integer *result,
                         const StringVal &str) {
    Length(ctx, result, str);
  }

  static void Lower(ExecutionContext *ctx, StringVal *result,
                    const StringVal &str);

  static void Upper(ExecutionContext *ctx, StringVal *result,
                    const StringVal &str);

  static void Replace(ExecutionContext *ctx, StringVal *result,
                      const StringVal &str, const StringVal &pattern,
                      const StringVal &replace);

  static void Reverse(ExecutionContext *ctx, StringVal *result,
                      const StringVal &str);

  static void Trim(ExecutionContext *ctx, StringVal *result,
                   const StringVal &str);

  static void Ltrim(ExecutionContext *ctx, StringVal *result,
                    const StringVal &str);

  static void Rtrim(ExecutionContext *ctx, StringVal *result,
                    const StringVal &str);
};

}  // namespace tpl::sql
