#pragma once

#include <sstream>
#include "function_builder.h"

namespace tpl::compiler {
using Block = std::string;
using Type = ast::Type;

class Value {

 public:
  Type &GetType() {
    return type_;
  }

  virtual std::string &GetString() {
    return name_;
  }

 private:
  friend class Function;
  std::string name_;
  Type type_;
};

class Constant : public Value {

  std::string &GetString() override {
    return value_;
  }

 private:
  friend class Function;
  std::string value_;
};

class Function {


  // TODO(tanujnay112) make it not copy strings around
  Block AssignValue(const Value *assignee, Value *val) {
    stream.clear();
    stream << assignee->name_ << "=";
    stream << val->GetString() << ";";
    Block retBlock = stream.str();
    blocks_.push_back(retBlock);
    return retBlock;
  }

  Block Call(const Function *fn, std::initializer_list<Value *> arguments) {

    //should I leave this to be checked in the TPL to AST compiler?
    TPL_ASSERT(fn->numArgs_ == arguments.size(), "Incorrect number of arguments");
    stream.clear();
    stream << fn->name_ << "(";
    bool first = true;
    for(Value *arg : arguments) {
      if(first) {
        first = false;
      }else{
        stream << ",";
      }
      stream << arg->GetString();
    }
    stream << ")" << ";";
    Block retBlock = stream.str();
    blocks_.push_back(retBlock);
    return retBlock;
  }

  Block Compile() {
    stream.clear();
    for(auto Block : blocks_) {
      stream << blocks_;
    }
    return stream.str();
  }

 private:
  std::stringstream stream;
  std::string name_;
  size_t numArgs_;
  std::vector<const Block&> blocks_;
};

}