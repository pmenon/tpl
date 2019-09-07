#include "vm/bytecode_module.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "ast/type.h"

namespace tpl::vm {

BytecodeModule::BytecodeModule(std::string name, std::vector<uint8_t> &&code,
                               std::vector<FunctionInfo> &&functions)
    : name_(std::move(name)), code_(std::move(code)), functions_(std::move(functions)) {}

namespace {

void PrettyPrintFuncInfo(std::ostream &os, const FunctionInfo &func) {
  os << "Function " << func.id() << " <" << func.name() << ">:" << std::endl;
  os << "  Frame size " << func.frame_size() << " bytes (" << func.num_params() << " parameter"
     << (func.num_params() > 1 ? "s, " : ", ") << func.locals().size() << " locals)" << std::endl;

  uint64_t max_local_len = 0;
  for (const auto &local : func.locals()) {
    max_local_len = std::max(max_local_len, static_cast<uint64_t>(local.name().length()));
  }
  for (const auto &local : func.locals()) {
    if (local.is_parameter()) {
      os << "    param  ";
    } else {
      os << "    local  ";
    }
    os << std::setw(max_local_len) << std::right << local.name() << ":  offset=" << std::setw(7)
       << std::left << local.offset() << " size=" << std::setw(7) << std::left << local.size()
       << " align=" << std::setw(7) << std::left << local.type()->alignment()
       << " type=" << std::setw(7) << std::left << ast::Type::ToString(local.type()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream &os, const BytecodeModule &module, const FunctionInfo &func,
                         BytecodeIterator &iter) {
  const uint32_t max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !iter.Done(); iter.Advance()) {
    Bytecode bytecode = iter.CurrentBytecode();

    // Print common bytecode info
    os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex
       << iter.GetPosition();
    os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len) << std::left
       << Bytecodes::ToString(bytecode);

    auto print_local = [&](const LocalVar local) {
      const auto *local_info = func.LookupLocalInfoByOffset(local.GetOffset());
      TPL_ASSERT(local_info, "No local at offset");

      os << "local=";
      if (local.GetAddressMode() == LocalVar::AddressMode::Address) {
        os << "&";
      }
      os << local_info->name();
    };

    for (uint32_t i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
      if (i != 0) os << "  ";
      switch (Bytecodes::GetNthOperandType(bytecode, i)) {
        case OperandType::None: {
          break;
        }
        case OperandType::Imm1: {
          auto imm = iter.GetImmediateOperand(i);
          os << "i8=" << imm;
          break;
        }
        case OperandType::Imm2: {
          auto imm = iter.GetImmediateOperand(i);
          os << "i16=" << imm;
          break;
        }
        case OperandType::Imm4: {
          auto imm = iter.GetImmediateOperand(i);
          os << "i32=" << imm;
          break;
        }
        case OperandType::Imm8: {
          auto imm = iter.GetImmediateOperand(i);
          os << "i64=" << imm;
          break;
        }
        case OperandType::UImm2: {
          auto imm = iter.GetUnsignedImmediateOperand(i);
          os << "u16=" << imm;
          break;
        }
        case OperandType::UImm4: {
          auto imm = iter.GetUnsignedImmediateOperand(i);
          os << "u32=" << imm;
          break;
        }
        case OperandType::JumpOffset: {
          auto target = iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, i) +
                        iter.GetJumpOffsetOperand(i);
          os << "target=0x" << std::right << std::setfill('0') << std::setw(6) << std::hex << target
             << std::dec;
          break;
        }
        case OperandType::Local: {
          print_local(iter.GetLocalOperand(i));
          break;
        }
        case OperandType::LocalCount: {
          std::vector<LocalVar> locals;
          uint16_t n = iter.GetLocalCountOperand(i, locals);
          for (uint16_t j = 0; j < n; j++) {
            print_local(locals[j]);
            os << "  ";
          }
          break;
        }
        case OperandType::FunctionId: {
          auto target = module.GetFuncInfoById(iter.GetFunctionIdOperand(i));
          os << "func=<" << target->name() << ">";
          break;
        }
      }
    }

    os << std::endl;
  }
}

void PrettyPrintFunc(std::ostream &os, const BytecodeModule &module, const FunctionInfo &func) {
  PrettyPrintFuncInfo(os, func);

  os << std::endl;

  auto iter = module.BytecodeForFunction(func);
  PrettyPrintFuncCode(os, module, func, iter);

  os << std::endl;
}

}  // namespace

void BytecodeModule::PrettyPrint(std::ostream &os) const {
  for (const auto &func : functions_) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace tpl::vm
