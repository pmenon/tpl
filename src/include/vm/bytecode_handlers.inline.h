#pragma once

#include <cstdint>

#include "util/macros.h"
#include "vm/types.h"

namespace tpl::vm {

////////////////////////////////////////////////////////////////////////////////
///
/// Integer Comparisons
///
////////////////////////////////////////////////////////////////////////////////

static inline ALWAYS_INLINE VmInt OpIntLt(VmInt l, VmInt r) { return l < r; }

static inline ALWAYS_INLINE VmInt OpIntLe(VmInt l, VmInt r) { return l <= r; }

static inline ALWAYS_INLINE VmInt OpIntEq(VmInt l, VmInt r) { return l == r; }

static inline ALWAYS_INLINE VmInt OpIntGt(VmInt l, VmInt r) { return l > r; }

static inline ALWAYS_INLINE VmInt OpIntGe(VmInt l, VmInt r) { return l >= r; }

////////////////////////////////////////////////////////////////////////////////
///
/// Integer Arithmetic
///
////////////////////////////////////////////////////////////////////////////////

static inline ALWAYS_INLINE VmInt OpIntNeg(VmInt l) { return -l; }

static inline ALWAYS_INLINE VmInt OpIntAdd(VmInt l, VmInt r) { return l + r; }

static inline ALWAYS_INLINE VmInt OpIntSub(VmInt l, VmInt r) { return l - r; }

static inline ALWAYS_INLINE VmInt OpIntMul(VmInt l, VmInt r) { return l * r; }

static inline ALWAYS_INLINE VmInt OpIntDiv(VmInt l, VmInt r) {
  TPL_ASSERT(r != 0, "Divisor expected to be non-zero");
  return l / r;
}

static inline ALWAYS_INLINE VmInt OpIntRem(VmInt l, VmInt r) {
  TPL_ASSERT(r != 0, "Divisor expected to be non-zero");
  return l % r;
}

////////////////////////////////////////////////////////////////////////////////
///
/// Integer Bit Shift
///
////////////////////////////////////////////////////////////////////////////////

static inline ALWAYS_INLINE VmInt OpIntShl(VmInt l, VmInt r) { return l << r; }

static inline ALWAYS_INLINE VmInt OpIntShr(VmInt l, VmInt r) {
  // Mask r to ensure 0 <= r < 32
  return l >> (r & 0x1f);
}

static inline ALWAYS_INLINE VmInt OpIntUshr(VmInt l, VmInt r) {
  // We need to ensure 0 <= r < 32
  return static_cast<VmUInt>(l) >> (r & 0x1f);
}

}  // namespace tpl::vm