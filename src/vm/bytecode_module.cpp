#include "vm/bytecode_module.h"

#include <iomanip>
#include <iostream>

#include "ast/type.h"

namespace tpl::vm {

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
       << " size=" << std::setw(7) << std::left << local.Size()
       << " align=" << std::setw(7) << std::left << local.type()->alignment()
       << " type=" << std::setw(7) << std::left
       << ast::Type::ToString(local.type()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream &os, const FunctionInfo &func,
                         BytecodeIterator &bytecode_iter) {
  const u32 max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !bytecode_iter.Done(); bytecode_iter.Advance()) {
    os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex
       << bytecode_iter.current_offset();
    os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len)
       << std::left << Bytecodes::ToString(bytecode_iter.CurrentBytecode())
       << std::endl;
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
  for (const auto &func : functions()) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace tpl::vm