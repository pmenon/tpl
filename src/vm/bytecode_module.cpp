#include "vm/bytecode_module.h"

#include <iomanip>
#include <iostream>

#include "llvm/ADT/SmallVector.h"

#include "ast/type.h"

namespace tpl::vm {

BytecodeModule::BytecodeModule(std::string name, std::vector<u8> &&code,
                               std::vector<FunctionInfo> &&functions)
    : name_(std::move(name)),
      code_(std::move(code)),
      functions_(std::move(functions)),
      trampolines_(functions_.size()) {
  for (const auto &func : functions_) {
    CreateFunctionTrampoline(func.id());
  }
}

void BytecodeModule::CreateFunctionTrampoline(const FunctionInfo &func,
                                              Trampoline &trampoline) {
  llvm::SmallVector<u8, 256> code;
  code.push_back(0xf4);

  // Create the memory
  std::error_code error;
  u32 flags = llvm::sys::Memory::ProtectionFlags::MF_READ |
              llvm::sys::Memory::ProtectionFlags::MF_WRITE |
              llvm::sys::Memory::ProtectionFlags::MF_EXEC;
  llvm::sys::MemoryBlock mem = llvm::sys::Memory::allocateMappedMemory(
      code.size(), nullptr, flags, error);
  if (error) {
    LOG_ERROR("There was an error allocating executable memory {}",
              error.message());
    return;
  }

  // Copy code into allocated memory
  std::memcpy(mem.base(), code.data(), code.size());

  // Done
  trampoline = Trampoline(mem);
}

void BytecodeModule::CreateFunctionTrampoline(FunctionId func_id) {
  // If a trampoline has already been setup, don't bother
  if (trampolines_[func_id].GetTrampolineCode() != nullptr) {
    LOG_DEBUG("Function {} has a trampoline; will not recreate", func_id);
    return;
  }

  // Lookup the function
  const auto *func_info = GetFuncInfoById(func_id);
  TPL_ASSERT(func_info != nullptr, "Function doesn't exist");

  // Create the trampoline for the function
  Trampoline trampoline;
  CreateFunctionTrampoline(*func_info, trampoline);

  // Mark available
  trampolines_[func_id] = trampoline;
}

namespace {

void PrettyPrintFuncInfo(std::ostream &os, const FunctionInfo &func) {
  os << "Function " << func.id() << " <" << func.name() << ">:" << std::endl;
  os << "  Frame size " << func.frame_size() << " bytes (" << func.num_params()
     << " parameter" << (func.num_params() > 1 ? "s, " : ", ")
     << func.locals().size() << " locals)" << std::endl;

  u64 max_local_len = 0;
  for (const auto &local : func.locals()) {
    max_local_len = std::max(max_local_len, (u64)local.name().length());
  }
  for (const auto &local : func.locals()) {
    if (local.IsParameter()) {
      os << "    param  ";
    } else {
      os << "    local  ";
    }
    os << std::setw(max_local_len) << std::right << local.name()
       << ":  offset=" << std::setw(7) << std::left << local.offset()
       << " size=" << std::setw(7) << std::left << local.Size()
       << " align=" << std::setw(7) << std::left << local.type()->alignment()
       << " type=" << std::setw(7) << std::left
       << ast::Type::ToString(local.type()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream &os, const FunctionInfo &func,
                         BytecodeIterator &iter) {
  const u32 max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !iter.Done(); iter.Advance()) {
    Bytecode bytecode = iter.CurrentBytecode();

    // Print common bytecode info
    os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex
       << iter.GetPosition();
    os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len)
       << std::left << Bytecodes::ToString(bytecode) << std::endl;
  }
}

void PrettyPrintFunc(std::ostream &os, const BytecodeModule &module,
                     const FunctionInfo &func) {
  PrettyPrintFuncInfo(os, func);

  os << std::endl;

  auto iter = module.BytecodeForFunction(func);
  PrettyPrintFuncCode(os, func, iter);

  os << std::endl;
}

}  // namespace

void BytecodeModule::PrettyPrint(std::ostream &os) {
  for (const auto &func : functions_) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace tpl::vm
