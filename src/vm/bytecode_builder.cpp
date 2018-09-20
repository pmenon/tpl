#include "vm/bytecode_builder.h"

#include "logging/logger.h"
#include "vm/bytecode_function.h"

namespace tpl::vm {

BytecodeBuilder::BytecodeBuilder(
    std::unique_ptr<ConstantsArrayBuilder> &&constants_builder)
    : constants_builder_(std::move(constants_builder)) {}

BytecodeBuilder &BytecodeBuilder::LoadLiteral(VmInt val) {
  auto index = GetConstantPoolEntry(val);
  OutputLoadConstant(index);
  return *this;
}

BytecodeBuilder &BytecodeBuilder::UnaryOperation(
    parsing::Token::Type unary_op) {
  switch (unary_op) {
    case parsing::Token::Type::BANG: {
      LoadLiteral(0).OutputCode(Bytecode::Eq);
      break;
    }
    default: {
      LOG_ERROR("'{}' is not a unary operation!",
                parsing::Token::Name(unary_op));
      break;
    }
  }

  return *this;
}

BytecodeBuilder &BytecodeBuilder::BinaryOperation(parsing::Token::Type bin_op) {
  switch (bin_op) {
    case parsing::Token::Type::PLUS: {
      OutputCode(Bytecode::Add);
      break;
    }
    case parsing::Token::Type::MINUS: {
      OutputCode(Bytecode::Sub);
      break;
    }
    case parsing::Token::Type::STAR: {
      OutputCode(Bytecode::Mul);
      break;
    }
    case parsing::Token::Type::SLASH: {
      OutputCode(Bytecode::Div);
      break;
    }
    case parsing::Token::Type::PERCENT: {
      OutputCode(Bytecode::Rem);
      break;
    }
    default: {
      LOG_ERROR("'{}' is not a binary operation!",
                parsing::Token::Name(bin_op));
      break;
    }
  }

  return *this;
}

void BytecodeBuilder::OutputLoadConstant(uint16_t index) {
  OutputCode(Bytecode::Ldc);
  TPL_ASSERT(index <= std::numeric_limits<uint16_t>::max(),
             "Constant index must be less than 2^16");
  bytecodes_.push_back(static_cast<uint8_t>(index >> 8));
  bytecodes_.push_back(static_cast<uint8_t>(index & 0xff));
}

BytecodeBuilder &BytecodeBuilder::Return() {
  OutputCode(Bytecode::Ret);
  return *this;
}

uint32_t BytecodeBuilder::GetConstantPoolEntry(VmInt val) {
  return constants_array_builder().Insert(val);
}

std::unique_ptr<BytecodeFunction> BytecodeBuilder::Build() {
  auto code = std::make_unique<uint8_t[]>(bytecodes_.size());
  TPL_MEMCPY(code.get(), bytecodes_.data(),
             sizeof(uint8_t) * bytecodes_.size());

  return std::make_unique<BytecodeFunction>(constants_array_builder().ToArray(),
                                            std::move(code));
}

}  // namespace tpl::vm