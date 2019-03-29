#include "vm/bytecode_module.h"

#include <iomanip>
#include <iostream>
#include <numeric>

#define XBYAK_NO_OP_NAMES
#include "xbyak/xbyak.h"

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

namespace {

struct TrampolineGenerator : public Xbyak::CodeGenerator {
  /// Generate trampoline code for the given function in the given module
  TrampolineGenerator(const BytecodeModule &module,
                      const FunctionInfo &func_info, void *mem)
      : Xbyak::CodeGenerator(Xbyak::DEFAULT_MAX_CODE_SIZE, mem),
        module_(module),
        func_(func_info) {
    // Push call args onto stack
    AdjustStackAndPushCallerArgs();
    //
    InvokeVMFunction();
    //
    RestoreStackAndReturn();
  }

  u32 ComputeRequiredStackSpace() const {
    // TODO(pmenon): Fix for non-integer args and args larger than 8-bytes
    const ast::FunctionType *func_type = func_.func_type();

    // First, we need to at least store all legitimate call arguments onto the
    // stack
    u32 required_stack_space = func_type->num_params() * sizeof(intptr_t);

    // If the function has a return type, we need to allocate a temporary
    // return value and a pointer to the return value on the stack as well
    if (const ast::Type *ret_type = func_type->return_type();
        !ret_type->IsNilType()) {
      // Space for the temporary return value
      required_stack_space += ret_type->size();
      // Space for the pointer to the return value
      required_stack_space += sizeof(intptr_t);
    }

    return required_stack_space;
  }

  // This function pushes all caller arguments onto the stack. After this call
  // we want the stack to look as follows:
  //
  //            |                   |
  //  Old SP -> +-------------------+ (Higher address)
  //            |       arg N       |
  //            +-------------------+
  //            |        ...        |
  //            +-------------------+
  //            |       arg 1       |
  //            +-------------------+
  //        +-- |  Pointer to 'rv'  |
  //        |   +-------------------+
  //        +-->| Return Value 'rv' |
  //      SP -> +-------------------+ (Lower address)
  //            |                   |
  //
  // If the function doesn't return anything, then no return value is allocated
  // on the stack, only the formal function arguments are pushed.
  //
  void AdjustStackAndPushCallerArgs() {
    const Xbyak::Reg64 arg_regs[] = {rdi, rsi, rdx, rcx, r8, r9};

    // Bump stack
    sub(rsp, ComputeRequiredStackSpace());

    // Stack displacement used to store things on the stack
    u32 displacement = 0;

    const ast::FunctionType *func_type = func_.func_type();
    if (const ast::Type *ret_type = func_type->return_type();
        !ret_type->IsNilType()) {
      displacement += ret_type->size();
      mov(ptr[rsp + displacement], rsp);
      displacement += sizeof(intptr_t);
    }

    // Push the arguments onto the stack
    for (u32 idx = 0; idx < func_type->num_params();
         idx++, displacement += sizeof(intptr_t)) {
      mov(ptr[rsp + displacement], arg_regs[idx]);
    }
  }

  void InvokeVMFunction() {
    const ast::FunctionType *func_type = func_.func_type();
    const u32 ret_type_size = func_type->return_type()->size();

    // We're going to call InvokeFunctionWrapper(module, func, args). Thus, we
    // need to ensure: RDI=module*, RSI=func*, RDX=args*
    mov(rdi, reinterpret_cast<std::size_t>(&module_));
    mov(rsi, func_.id());
    lea(rdx, ptr[rsp + ret_type_size]);

    // Make the call. Move the address of InvokeFunctionWrapper() into RAX and
    // emit a call instruction
    mov(rax, reinterpret_cast<std::size_t>(&VM::InvokeFunction));
    call(rax);
  }

  void RestoreStackAndReturn() {
    add(rsp, ComputeRequiredStackSpace());
    ret();
  }

 private:
  const BytecodeModule &module_;
  const FunctionInfo &func_;
};

}  // namespace

void BytecodeModule::CreateFunctionTrampoline(const FunctionInfo &func,
                                              Trampoline &trampoline) {
  // Allocate memory
  std::error_code error;
  u32 flags = llvm::sys::Memory::ProtectionFlags::MF_READ |
              llvm::sys::Memory::ProtectionFlags::MF_WRITE;
  llvm::sys::MemoryBlock mem =
      llvm::sys::Memory::allocateMappedMemory(1 << 12, nullptr, flags, error);
  if (error) {
    LOG_ERROR("There was an error allocating executable memory {}",
              error.message());
    return;
  }

  // Generate code
  TrampolineGenerator generator(*this, func, mem.base());

  // Now that the code's been generated and finalized, let's remove write
  // protections and just make is read+exec.
  llvm::sys::Memory::protectMappedMemory(
      mem, llvm::sys::Memory::ProtectionFlags::MF_READ |
               llvm::sys::Memory::ProtectionFlags::MF_EXEC);

  // Done
  trampoline = Trampoline(llvm::sys::OwningMemoryBlock(mem));
}

void BytecodeModule::CreateFunctionTrampoline(FunctionId func_id) {
  // If a trampoline has already been setup, don't bother
  if (trampolines_[func_id].GetCode() != nullptr) {
    LOG_DEBUG("Function {} has a trampoline; will not recreate", func_id);
    return;
  }

  // Lookup the function
  const auto *func_info = GetFuncInfoById(func_id);

  // Create the trampoline for the function
  Trampoline trampoline;
  CreateFunctionTrampoline(*func_info, trampoline);

  // Mark available
  trampolines_[func_id] = std::move(trampoline);
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
    if (local.is_parameter()) {
      os << "    param  ";
    } else {
      os << "    local  ";
    }
    os << std::setw(max_local_len) << std::right << local.name()
       << ":  offset=" << std::setw(7) << std::left << local.offset()
       << " size=" << std::setw(7) << std::left << local.size()
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
