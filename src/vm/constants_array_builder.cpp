#include "vm/constants_array_builder.h"

namespace tpl::vm {

ConstantsArrayBuilder::index_t ConstantsArrayBuilder::Insert(VmInt val) {
  auto iter = ints_.find(val);
  if (iter != ints_.end()) {
    return iter->second;
  }

  auto index = AllocateIndex(VmObjType::Int, val);
  ints_[val] = index;
  return index;
}

ConstantsArrayBuilder::index_t ConstantsArrayBuilder::Insert(VmString val) {
  auto iter = strings_.find(val);
  if (iter != strings_.end()) {
    return iter->second;
  }

  auto index = AllocateIndex(VmObjType::Reference, val);
  strings_[val] = index;
  return index;
}

std::unique_ptr<ConstantsArray> ConstantsArrayBuilder::ToArray() {
  auto types = std::make_unique<VmObjType[]>(types_.size());
  auto constants = std::make_unique<intptr_t[]>(types_.size());

  TPL_MEMCPY(types.get(), types_.data(), types_.size() * sizeof(VmObjType));
  TPL_MEMCPY(constants.get(), constants_.data(),
             types_.size() * sizeof(intptr_t));

  // We can't use make_unique() because we're calling a private constructor
  return std::unique_ptr<ConstantsArray>(
      new ConstantsArray(std::move(types), std::move(constants)));
}

#ifndef NDEBUG
void ConstantsArrayBuilder::CheckAllElementsAreUnique() {
  // TODO(pmenon): Implement me
}
#endif

}  // namespace tpl::vm