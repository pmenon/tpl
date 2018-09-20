#pragma once

#include <memory>
#include <vector>

#include "util/macros.h"
#include "vm/types.h"

namespace tpl::vm {

class ConstantsArray {
 public:
  friend class ConstantsArrayBuilder;

  DISALLOW_COPY_AND_MOVE(ConstantsArray);

  VmObjType TypeAt(uint32_t idx) const { return types_[idx]; }

  template <typename T>
  T &ObjAt(uint32_t idx) const {
    return reinterpret_cast<T &>(constants_[idx]);
  }

 private:
  ConstantsArray(std::unique_ptr<VmObjType[]> &&types,
                 std::unique_ptr<intptr_t[]> &&constants)
      : types_(std::move(types)), constants_(std::move(constants)) {}

 private:
  std::unique_ptr<VmObjType[]> types_;
  std::unique_ptr<intptr_t[]> constants_;
};

}  // namespace tpl::vm