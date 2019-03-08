#pragma once

#include "util/common.h"

namespace tpl::sql {

struct Val {
  bool null;

  explicit Val(bool _null) noexcept : null(_null) {}
};

struct BoolVal : public Val {
  bool val;

  explicit BoolVal(bool null, bool v) noexcept : Val(null), val(v) {}

  bool ForceTruth() const noexcept { return !null && val; }
};

/// An integral SQL value
struct Integer : public Val {
  union {
    i16 smallint;
    i32 integer;
    i64 bigint;
  } val;

  Integer(bool null, i64 v) noexcept : Val(null) { val.bigint = v; }
};

/// A decimal SQL value
struct Decimal : public Val {
  u64 val;
  u32 scale_factor;
};

/// A SQL string
struct String {
  u8 *str;
  u32 len;
};

}  // namespace tpl::sql
