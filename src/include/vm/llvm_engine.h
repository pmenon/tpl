#pragma once

namespace tpl::vm {

/// The interface to LLVM to perform JIT of TPL bytecode
class LLVMEngine {
 public:
  /// Initialize the whole LLVM subsystem
  static void Initialize();

  /// Shutdown the whole LLVM subsystem
  static void Shutdown();
};

}  // namespace tpl::vm