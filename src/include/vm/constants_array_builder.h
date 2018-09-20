#pragma once

#include <vector>

#include <llvm/ADT/DenseMap.h>

#include "vm/constants_array.h"
#include "vm/types.h"

namespace tpl::vm {

class ConstantsArrayBuilder {
 public:
  uint32_t Insert(VmInt val);
  uint32_t Insert(VmString val);

  std::unique_ptr<ConstantsArray> ToArray();

#ifndef NDEBUG
  void CheckAllElementsAreUnique();
#else
  // No-op in non-debug mode
  void CheckAllElementsAreUnique() {}
#endif

 private:
  using index_t = uint32_t;

  template <typename T>
  index_t AllocateIndex(VmObjType type, const T &val) {
    static_assert(sizeof(T) <= sizeof(intptr_t));
    types_.emplace_back(type);
    constants_.emplace_back(reinterpret_cast<const intptr_t &>(val));
    return static_cast<index_t>(constants_.size() - 1);
  }

 private:
  // The actual array of constants
  std::vector<VmObjType> types_;
  std::vector<intptr_t> constants_;

  // Indexes over the constants array for int and string elements
  llvm::DenseMap<VmInt, index_t> ints_;
  llvm::DenseMap<VmString, index_t> strings_;
};

}  // namespace tpl::vm
