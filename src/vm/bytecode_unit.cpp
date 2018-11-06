#include "vm/bytecode_unit.h"

#include <iomanip>
#include <iostream>
#include <map>

#include "ast/type.h"

namespace tpl::vm {

namespace {

void PrettyPrintFuncInfo(std::ostream &os, const FunctionInfo &func) {
  os << "Function " << func.id() << " <" << func.name() << ">:" << std::endl;
  os << "  Frame size " << func.frame_size() << " bytes ("
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
  std::map<u32, u32> labels;
  u32 label_index = 0;
  const u32 max_inst_len = Bytecodes::MaxBytecodeNameLength();

  // TODO(siva): This is hacky. Fix me.
  // Initial pass to populate the labels
  BytecodeIterator bytecode_iter_copy = bytecode_iter;
  for (; !bytecode_iter_copy.Done(); bytecode_iter_copy.Advance()) {
    auto current_bytecode = bytecode_iter_copy.current_bytecode();
    if (Bytecodes::IsJump(current_bytecode)) {
      u32 idx = Bytecodes::IsConditionalJump(current_bytecode) ? 1 : 0;
      auto offset = bytecode_iter_copy.current_offset() +
                    Bytecodes::GetNthOperandOffset(current_bytecode, idx);
      auto skip = bytecode_iter_copy.GetNthOperand<u16>(idx);
      if (Bytecodes::IsBackwardJump(current_bytecode)) {
        offset -= skip;
      } else {
        offset += skip;
      }
      if (labels.find(offset) == labels.end()) {
        labels[offset] = label_index++;
      }
    }
  }

  for (; !bytecode_iter.Done(); bytecode_iter.Advance()) {
    auto current_offset = bytecode_iter.current_offset();
    auto current_bytecode = bytecode_iter.current_bytecode();
    if (labels.find(current_offset) != labels.end()) {
      os << std::right << std::setw(3) << "L"
         << std::left << std::setw(3) << labels[current_offset] << ": ";
    }
    else
      os << std::setw(8) << " ";
    os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex
       << (current_offset - bytecode_iter.start_offset());
    os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len)
       << std::left << Bytecodes::ToString(current_bytecode);

    if (Bytecodes::IsJump(current_bytecode)) {
      u32 idx = Bytecodes::IsConditionalJump(current_bytecode) ? 1 : 0;
      auto offset = current_offset +
                    Bytecodes::GetNthOperandOffset(current_bytecode, idx);
      auto skip = bytecode_iter.GetNthOperand<u16>(idx);
      if (Bytecodes::IsBackwardJump(current_bytecode)) {
        offset -= skip;
      } else {
        offset += skip;
      }
      os << " L" << labels[offset];
    }

    os << std::endl;
  }
}

void PrettyPrintFunc(std::ostream &os, const BytecodeUnit &unit,
                     const FunctionInfo &func) {
  PrettyPrintFuncInfo(os, func);

  os << std::endl;

  auto iter = unit.BytecodeForFunction(func);
  PrettyPrintFuncCode(os, func, iter);

  os << std::endl;
}

}  // namespace

void BytecodeUnit::PrettyPrint(std::ostream &os) {
  for (const auto &func : functions()) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace tpl::vm