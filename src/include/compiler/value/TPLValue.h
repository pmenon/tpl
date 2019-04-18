#pragma once

#include <compiler/function_builder.h>
#include "ast/type.h"
namespace tpl::compiler {

class TPLValgen;

class TPLValue {
 friend class TPLValgen;

 private:
  explicit TPLValue(ast::Type type, int id) type_{type}, id_{id} {};
  tpl::ast::Type type_;
  int id_;
};

class TPLValgen {

  TPLValgen() currId{0} {};

  TPLValue *CreateVal(ast::Type type) {
    TPLValue *newVal = new TPLValue(type, currId++);
    return newVal;
  }

 private:
  int currId;
};

class FunctionVal : public TPLValue {

  void AddBlock(const Block &b) {
    blocks_.push_back(b);
  }

  Block Compile() {

  }

 private:
  explicit FunctionVal(int id, int numArgs) : type_{ast::FunctionType}, id_{id}, numArgs_{numArgs} {};
  int numArgs_;

  std::vector<Block&> blocks_;
};


} //namespace tpl::compiler
