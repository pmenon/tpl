#pragma once

#include <cstdint>

namespace tpl::vm {

enum class VmObjType : uint8_t { Int, Float, Reference };

using VmInt = int32_t;
using VmUInt = uint32_t;
using VmFloat = float;
using VmString = const char *;
using VmReference = void *;

#if 0
struct VmValue {
  VmObjType type;
  union {
    VmInt int_val;
    VmFloat float_val;
    VmString str_val;
    VmReference obj_val;
  };
};
#endif
using VmValue = intptr_t;

}  // namespace tpl::vm