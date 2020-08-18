#pragma once

#include <limits>

namespace tpl::vm {

/**
 * Function IDs are 16-bit numbers. These are used in encoded bytecode.
 */
using FunctionId = uint16_t;

/**
 * An invalid function ID.
 */
static constexpr FunctionId kInvalidFuncId = std::numeric_limits<FunctionId>::max();

/**
 * An enumeration capturing different execution methods and optimization levels.
 */
enum class ExecutionMode : uint8_t {
  // Always execute in interpreted mode
  Interpret,
  // Execute in interpreted mode, but trigger a compilation asynchronously. As
  // compiled code becomes available, seamlessly swap it in and execute mixed
  // interpreter and compiled code.
  Adaptive,
  // Compile and generate all machine code before executing the function
  Compiled
};

}  // namespace tpl::vm
