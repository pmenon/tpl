#pragma once

#include <limits>

#include "sql/value.h"

namespace tpl::sql {

class ExecutionContext;

/**
 * Utility class to handle SQL string manipulations.
 */
class StringFunctions {
 public:
  // Delete to force only static functions
  StringFunctions() = delete;

  static void CharLength(ExecutionContext *ctx, Integer *result, const StringVal &str) {
    Length(ctx, result, str);
  }

  static void Left(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                   const Integer &n);

  static void Length(ExecutionContext *ctx, Integer *result, const StringVal &str);

  static void Lower(ExecutionContext *ctx, StringVal *result, const StringVal &str);

  static void Lpad(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                   const Integer &len, const StringVal &pad);

  static void Ltrim(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                    const StringVal &chars);

  static void Ltrim(ExecutionContext *ctx, StringVal *result, const StringVal &str);

  static void Repeat(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                     const Integer &n);

  static void Reverse(ExecutionContext *ctx, StringVal *result, const StringVal &str);

  static void Right(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                    const Integer &n);

  static void Rpad(ExecutionContext *ctx, StringVal *result, const StringVal &str, const Integer &n,
                   const StringVal &pad);

  static void Rtrim(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                    const StringVal &chars);

  static void Rtrim(ExecutionContext *ctx, StringVal *result, const StringVal &str);

  static void SplitPart(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                        const StringVal &delim, const Integer &field);

  static void Substring(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                        const Integer &pos, const Integer &len);

  static void Substring(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                        const Integer &pos) {
    Substring(ctx, result, str, pos, Integer(std::numeric_limits<int64_t>::max()));
  }

  static void Trim(ExecutionContext *ctx, StringVal *result, const StringVal &str,
                   const StringVal &chars);

  static void Trim(ExecutionContext *ctx, StringVal *result, const StringVal &str);

  static void Upper(ExecutionContext *ctx, StringVal *result, const StringVal &str);
};

}  // namespace tpl::sql
