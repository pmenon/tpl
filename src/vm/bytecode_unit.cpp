#include "vm/bytecode_unit.h"

#include <iomanip>
#include <iostream>

#include "ast/type.h"

namespace tpl::vm {

namespace {

void PrettyPrintFuncInfo(std::ostream &os, const FunctionInfo &func) {
  os << "Function " << func.id() << " <" << func.name() << ">:" << std::endl;
  os << "  Frame size " << func.frame_size() << " bytes ("
     << func.locals().size() << " locals)" << std::endl;

  u64 max_local_len = 0;
  for (const auto &reg : func.locals()) {
    max_local_len = std::max(max_local_len, reg.name().length());
  }
  for (const auto &reg : func.locals()) {
    os << "    local  ";
    os << std::setw(max_local_len) << std::right << reg.name()
       << ":  offset=" << std::setw(7) << std::left << reg.offset()
       << " size=" << std::setw(7) << std::left << reg.Size()
       << " align=" << std::setw(7) << std::left << reg.type()->alignment()
       << " type=" << std::setw(7) << std::left
       << ast::Type::ToString(reg.type()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream &os, const FunctionInfo &func,
                         BytecodeIterator &bytecode_iter) {
  const u32 max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !bytecode_iter.Done(); bytecode_iter.Advance()) {
    os << "  0x000" << std::hex
       << (bytecode_iter.current_offset() - bytecode_iter.start_offset())
       << "    " << std::dec << std::setw(max_inst_len) << std::left
       << Bytecodes::ToString(bytecode_iter.current_bytecode()) << std::endl;
  }
}

void PrettyPrintFunc(std::ostream &os, const BytecodeUnit &unit,
                     FunctionId func_id) {
  const auto &func = unit.GetFunction(func_id);
  PrettyPrintFuncInfo(os, func);

  os << std::endl;

  auto iter = unit.BytecodeForFunction(func_id);
  PrettyPrintFuncCode(os, func, iter);
}

}  // namespace

void BytecodeUnit::PrettyPrint(std::ostream &os) {
  for (FunctionId id = 0; id < num_functions(); id++) {
    PrettyPrintFunc(os, *this, id);
  }
}

}  // namespace tpl::vm