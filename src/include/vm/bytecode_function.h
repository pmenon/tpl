#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "vm/constants_array.h"

namespace tpl::vm {

class BytecodeFunction {
 public:
  BytecodeFunction(std::unique_ptr<ConstantsArray> &&constants,
                   std::unique_ptr<uint8_t[]> &&code)
      : constants_(std::move(constants)), code_(std::move(code)) {}

  const uint8_t *const code() const { return code_.get(); }

  uint32_t max_locals() const { return max_locals_; }

  uint32_t max_stack() const { return 1024; /*return max_stack_;*/ }

  const ConstantsArray &constants() const { return *constants_; }

 private:
  std::unique_ptr<ConstantsArray> constants_;
  std::unique_ptr<uint8_t[]> code_;
  uint32_t max_locals_;
  uint32_t max_stack_;
};

}  // namespace tpl::vm