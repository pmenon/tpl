#pragma once

#include <limits>

#include "util/macros.h"

namespace tpl::vm {

class BytecodeLabel {
  static constexpr const std::size_t kInvalidPos =
      std::numeric_limits<std::size_t>::max();

 public:
  BytecodeLabel() : bytecode_pos_(kInvalidPos), bound_(false) {}

  bool is_bound() const { return bound_; }

  std::size_t bytecode_position() const { return bytecode_pos_; }

 private:
  friend class BytecodeEmitter;

  void BindTo(std::size_t pos) {
    TPL_ASSERT(!is_bound() && bytecode_position() != kInvalidPos,
               "Cannot rebind an already bound label!");
    bound_ = true;
    bytecode_pos_ = pos;
  }

 private:
  std::size_t bytecode_pos_;
  bool bound_;
};

}  // namespace tpl::vm