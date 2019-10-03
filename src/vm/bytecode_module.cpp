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
  os << "Function " << func.GetId() << " <" << func.GetName() << ">:" << std::endl;
  os << "  Frame size " << func.GetFrameSize() << " bytes (" << func.GetParamsCount()
     << " parameter" << (func.GetParamsCount() > 1 ? "s, " : ", ") << func.GetLocals().size()
     << " locals)" << std::endl;

  uint64_t max_local_len = 0;
  for (const auto &local : func.GetLocals()) {
    max_local_len = std::max(max_local_len, static_cast<uint64_t>(local.GetName().length()));
  }
  for (const auto &local : func.GetLocals()) {
    if (local.IsParameter()) {
      os << "    param  ";
    } else {
      os << "    local  ";
    }
    os << std::setw(max_local_len) << std::right << local.GetName() << ":  offset=" << std::setw(7)
       << std::left << local.GetOffset() << " size=" << std::setw(7) << std::left << local.GetSize()
       << " align=" << std::setw(7) << std::left << local.GetType()->alignment()
       << " type=" << std::setw(7) << std::left << ast::Type::ToString(local.GetType())
       << std::endl;
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
      os << local_info->GetName();
    };

    for (uint32_t i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
      if (i != 0) os << "  ";
      switch (Bytecodes::GetNthOperandType(bytecode, i)) {
        case OperandType::None: {
          break;
        }
        case OperandType::Imm1: {
          auto imm = iter.GetImmediateIntegerOperand(i);
          os << "i8=" << imm;
          break;
        }
        case OperandType::Imm2: {
          auto imm = iter.GetImmediateIntegerOperand(i);
          os << "i16=" << imm;
          break;
        }
        case OperandType::Imm4: {
          auto imm = iter.GetImmediateIntegerOperand(i);
          os << "i32=" << imm;
          break;
        }
        case OperandType::Imm8: {
          auto imm = iter.GetImmediateIntegerOperand(i);
          os << "i64=" << imm;
          break;
        }
        case OperandType::Imm4F: {
          auto imm = iter.GetImmediateFloatOperand(i);
          os << "f32=" << std::fixed << std::setprecision(2) << imm << std::dec;
          break;
        }
        case OperandType::Imm8F: {
          auto imm = iter.GetImmediateFloatOperand(i);
          os << "f64=" << std::fixed << std::setprecision(2) << imm << std::dec;
          break;
        }
        case OperandType::UImm2: {
          auto imm = iter.GetUnsignedImmediateIntegerOperand(i);
          os << "u16=" << imm;
          break;
        }
        case OperandType::UImm4: {
          auto imm = iter.GetUnsignedImmediateIntegerOperand(i);
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
          os << "func=<" << target->GetName() << ">";
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
