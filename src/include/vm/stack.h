#pragma once

#include <cstdint>
#include <memory>

#include "util/macros.h"
#include "vm/types.h"

namespace tpl::vm {

namespace internal {

/*
 * A generic view over an array of bytes. Used by stacks and local-variable
 * arrays because there is a good amount of shared behaviour.
 */
class SlotArray {
 public:
  explicit SlotArray(uint32_t capacity)
      : slots_(std::make_unique<uintptr_t[]>(capacity)), capacity_(capacity) {}

  DISALLOW_COPY_AND_MOVE(SlotArray);

  template <typename T>
  T &SlotAs(uint32_t idx) {
    TPL_ASSERT(idx < capacity(), "Out-of-range slot array access!");
    return reinterpret_cast<T &>(slots_[idx]);
  }

  template <typename T>
  const T &SlotAs(uint32_t idx) const {
    TPL_ASSERT(idx < capacity(), "Out-of-range slot array access!");
    return reinterpret_cast<const T &>(slots_[idx]);
  }

  uint32_t capacity() const { return capacity_; }

 private:
  std::unique_ptr<uintptr_t[]> slots_;
  uint32_t capacity_;
};

}  // namespace internal

/*
 * A custom stack implementation
 */
class Stack {
 public:
  explicit Stack(uint32_t size);

  DISALLOW_COPY_AND_MOVE(Stack);

  VmInt PopInt();
  VmFloat PopFloat();
  VmReference PopReference();

  VmInt TopInt() const;
  VmFloat TopFloat() const;
  VmReference TopReference() const;

  void PushInt(VmInt val);
  void PushFloat(VmFloat val);
  void PushReference(VmReference val);

  void SetTopInt(VmInt val);
  void SetTopFloat(VmFloat val);
  void SetTopReference(VmReference val);

  uint32_t capacity() const { return slots_.capacity(); }
  uint32_t size() const { return sp_; }

 private:
  internal::SlotArray slots_;
  uint32_t sp_;
};

/*
 * A class to encapsulated an array of local variables
 */
class Locals {

};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline Stack::Stack(uint32_t size) : slots_(size), sp_(0) {}

inline VmInt Stack::PopInt() {
  TPL_ASSERT(sp_ > 0, "Stack overflow");
  return slots_.SlotAs<VmInt>(--sp_);
}

inline VmFloat Stack::PopFloat() {
  TPL_ASSERT(sp_ > 0, "Stack overflow");
  return slots_.SlotAs<VmFloat>(--sp_);
}

inline VmReference Stack::PopReference() {
  TPL_ASSERT(sp_ > 0, "Stack overflow");
  return slots_.SlotAs<VmReference>(--sp_);
}

inline VmInt Stack::TopInt() const { return slots_.SlotAs<VmInt>(sp_ - 1); }

inline VmFloat Stack::TopFloat() const {
  return slots_.SlotAs<VmFloat>(sp_ - 1);
}

inline VmReference Stack::TopReference() const {
  return slots_.SlotAs<VmReference>(sp_ - 1);
}

inline void Stack::PushInt(VmInt val) {
  TPL_ASSERT(sp_ < capacity(), "Stack overflow");
  slots_.SlotAs<VmInt>(sp_++) = val;
}

inline void Stack::PushFloat(VmFloat val) {
  TPL_ASSERT(sp_ < capacity(), "Stack overflow");
  slots_.SlotAs<VmFloat>(sp_++) = val;
}

inline void Stack::PushReference(VmReference val) {
  TPL_ASSERT(sp_ < capacity(), "Stack overflow");
  slots_.SlotAs<VmReference>(sp_++) = val;
}

inline void Stack::SetTopInt(VmInt val) { slots_.SlotAs<VmInt>(sp_ - 1) = val; }

inline void Stack::SetTopFloat(VmFloat val) {
  slots_.SlotAs<VmFloat>(sp_ - 1) = val;
}

inline void Stack::SetTopReference(VmReference val) {
  slots_.SlotAs<VmReference>(sp_ - 1) = val;
}

}  // namespace tpl::vm