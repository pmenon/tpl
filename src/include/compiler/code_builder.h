#pragma once

#include <sstream>
#include <ast/ast.h>
#include <ast/ast_node_factory.h>
#include "function_builder.h"

namespace tpl::compiler {
using Block = ast::AstNode;
using Type = ast::Expr;

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

  explicit Function(std::string name, std::vector<Value*> &&params, Type *retType) : name_(name) {
    region_ = std::make_shared<util::Region>();
    params_ = std::move(params);
    nodeFactory = std::make_unique<ast::AstNodeFactory>(region_.get());
    ast::Identifier identifier(name_.data());
    // TODO(tanujnay112) sort this dummy business out
    SourcePosition dummy;
    util::RegionVector<ast::FieldDecl *> fields(region_.get());
    fields.reserve(4);
    for(auto v : params_) {
      ast::Identifier ident(v->name_.data());
      ast::Expr *type = &v->GetType();
      fields.push_back(nodeFactory->NewFieldDecl(dummy, ident, type));
    }
    ast::FunctionTypeRepr *fnType = nodeFactory->NewFunctionType(dummy, std::move(fields), retType);
    ast::Identifier fnName(name.data());
    fnDecl = nodeFactory->NewFunctionDecl(dummy, fnName, nodeFactory->NewFunctionLitExpr(fnType, nullptr);
  }


  // TODO(tanujnay112) make it not copy strings around
  Block *AssignValue(const Value *assignee, Value *val) {
    nodeFactory->
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
  std::shared_ptr<util::Region> region_;
  std::unique_ptr<ast::AstNodeFactory> nodeFactory;
  std::string name_;
  std::vector<Value *> params_;
  ast::FunctionDecl *fnDecl;
  std::vector<Block*> blocks_;
};

}