#pragma once

#include <limits>
#include <string>

#include "sql/value.h"

namespace tpl::sql {

class ExecutionContext;

/**
 * Utility class to handle SQL string manipulations.
 */
class StringFunctions : public AllStatic {
 public:
  static void CharLength(Integer *result, ExecutionContext *ctx, const StringVal &str) {
    Length(result, ctx, str);
  }

  static void Concat(StringVal *result, ExecutionContext *ctx, const StringVal **inputs,
                     uint32_t num_inputs);

  static void Left(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                   const Integer &n);

  static void Length(Integer *result, ExecutionContext *ctx, const StringVal &str);

  static void Lower(StringVal *result, ExecutionContext *ctx, const StringVal &str);

  static void Lpad(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                   const Integer &len, const StringVal &pad);

  static void Ltrim(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                    const StringVal &chars);

  static void Ltrim(StringVal *result, ExecutionContext *ctx, const StringVal &str);

  static void Repeat(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                     const Integer &n);

  static void Reverse(StringVal *result, ExecutionContext *ctx, const StringVal &str);

  static void Right(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                    const Integer &n);

  static void Rpad(StringVal *result, ExecutionContext *ctx, const StringVal &str, const Integer &n,
                   const StringVal &pad);

  static void Rtrim(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                    const StringVal &chars);

  static void Rtrim(StringVal *result, ExecutionContext *ctx, const StringVal &str);

  static void SplitPart(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                        const StringVal &delim, const Integer &field);

  static void Substring(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                        const Integer &pos, const Integer &len);

  static void Substring(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                        const Integer &pos) {
    Substring(result, ctx, str, pos, Integer(std::numeric_limits<int64_t>::max()));
  }

  static void Trim(StringVal *result, ExecutionContext *ctx, const StringVal &str,
                   const StringVal &chars);

  static void Trim(StringVal *result, ExecutionContext *ctx, const StringVal &str);

  static void Upper(StringVal *result, ExecutionContext *ctx, const StringVal &str);

  static void Like(BoolVal *result, ExecutionContext *ctx, const StringVal &string,
                   const StringVal &pattern);
};

}  // namespace tpl::sql
