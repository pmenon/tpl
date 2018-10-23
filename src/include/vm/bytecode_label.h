#pragma once

#include <limits>

#include "util/macros.h"

namespace tpl::vm {

/**
 * A bytecode label represents a textual location in the bytecode and is often
 * used as the target of a jump instruction. When the label is bound, it becomes
 * an immutable reference to a location in the bytecode (accessible through @ref
 * offset()). If the label is a forward reference, @ref offset() will return
 * the bytecode location of the referring jump instruction.
 */
class BytecodeLabel {
  static constexpr const std::size_t kInvalidOffset =
      std::numeric_limits<std::size_t>::max();

 public:
  BytecodeLabel() : offset_(kInvalidOffset), bound_(false) {}

  bool is_bound() const { return bound_; }

  std::size_t offset() const { return offset_; }

  const std::vector<size_t> &referrer_offsets() const {
    return referrer_offsets_;
  }

  bool is_forward_target() const {
    return !is_bound() && referrer_offsets().size() != 0;
  }

 private:
  friend class BytecodeEmitter;

  void set_referrer(std::size_t offset) {
    TPL_ASSERT(!is_bound(),
               "Cannot set offset reference for already bound label");
    referrer_offsets_.push_back(offset);
  }

  void BindTo(std::size_t offset) {
    TPL_ASSERT(!is_bound() && offset != kInvalidOffset,
               "Cannot rebind an already bound label!");
    bound_ = true;
    offset_ = offset;
  }

 private:
  std::size_t offset_;
  std::vector<size_t> referrer_offsets_;
  bool bound_;
};

}  // namespace tpl::vm