#pragma once

#include "function_builder.h"

namespace tpl::compiler {
using Block = std::string;
using Type = ast::Type;

class Value {
 private:
  std::string name_;
  Type type_;
};

class Function {
 private:
  uint16_t numArgs_;
  std::vector<Block&> blocks_;
};

Block *AssignValue(const Value *assignee, const Value *val);

Block *Call(const Function *fn, std::initializer_list<Value*> arguments);

}