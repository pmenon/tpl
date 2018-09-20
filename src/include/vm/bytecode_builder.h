#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "parsing/token.h"
#include "vm/bytecodes.h"
#include "vm/constants_array_builder.h"
#include "vm/types.h"

namespace tpl::vm {

class BytecodeFunction;

class BytecodeBuilder {
 public:
  explicit BytecodeBuilder(
      std::unique_ptr<ConstantsArrayBuilder> &&constants_builder);

  BytecodeBuilder &LoadLiteral(VmInt val);

  BytecodeBuilder &UnaryOperation(parsing::Token::Type unary_op);

  BytecodeBuilder &BinaryOperation(parsing::Token::Type bin_op);

  BytecodeBuilder &Return();

  ConstantsArrayBuilder &constants_array_builder() {
    return *constants_builder_;
  }

  std::unique_ptr<BytecodeFunction> Build();

 private:
  void OutputCode(Bytecode bytecode) {
    bytecodes_.push_back(Bytecodes::ToByte(bytecode));
  }

  void OutputLoadConstant(uint16_t index);

  uint32_t GetConstantPoolEntry(VmInt val);

 private:
  std::unique_ptr<ConstantsArrayBuilder> constants_builder_;
  std::vector<uint8_t> bytecodes_;
};

}  // namespace tpl::vm
