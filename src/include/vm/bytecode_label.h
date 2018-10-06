#pragma once

#include <limits>

#include "util/macros.h"

namespace tpl::vm {

class BytecodeLabel {
  static constexpr const std::size_t kInvalidOffset =
      std::numeric_limits<std::size_t>::max();

 public:
  BytecodeLabel() : bytecode_offset_(kInvalidOffset), bound_(false) {}

  bool is_bound() const { return bound_; }

  std::size_t offset() const { return bytecode_offset_; }

  bool is_forward_target() const {
    return !is_bound() && offset() != kInvalidOffset;
  }

 private:
  friend class BytecodeEmitter;

  void set_reference(std::size_t offset) {
    TPL_ASSERT(!is_bound() && bytecode_offset_ == kInvalidOffset,
               "Cannot set offset reference for already bound label");
    bytecode_offset_ = offset;
  }

  void BindTo(std::size_t pos) {
    TPL_ASSERT(!is_bound() && offset() != kInvalidOffset,
               "Cannot rebind an already bound label!");
    bound_ = true;
    bytecode_offset_ = pos;
  }

 private:
  std::size_t bytecode_offset_;
  bool bound_;
};

}  // namespace tpl::vm