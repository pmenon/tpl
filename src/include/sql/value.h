#pragma once

#include "util/common.h"

namespace tpl::sql {

/**
 * An integral SQL value
 */
struct Integer {
  union {
    bool boolean;
    i16 smallint;
    i32 integer;
    i64 bigint;
  } val;
  bool null;
};

/**
 * A decimal SQL value
 */
struct Decimal {
  u64 val;
  u32 scale_factor;
  bool null;
};

/**
 * A SQL string
 */
struct String {
  u8 *str;
  u32 len;
};

}  // namespace tpl::sql