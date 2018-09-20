#pragma once

#include <cstdint>

namespace tpl::vm {

enum class VmObjType : uint8_t { Int, Float, Reference };

using VmInt = int32_t;
using VmUInt = uint32_t;
using VmFloat = float;
using VmString = const char *;
using VmReference = void *;

struct VmValue {
  VmObjType type;
  union {
    VmInt int_val;
    VmFloat float_val;
    VmString str_val;
    VmReference obj_val;
  };
};

}  // namespace tpl::vm