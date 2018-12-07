#include "vm/llvm_engine.h"

#include "llvm/Support/ManagedStatic.h"
#include "llvm/Support/TargetSelect.h"

namespace tpl::vm {

void LLVMEngine::Initialize() {
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
}

void LLVMEngine::Shutdown() { llvm::llvm_shutdown(); }

}  // namespace tpl::vm