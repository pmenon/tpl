#pragma once

#include <sstream>
#include <ast/ast.h>
#include <ast/ast_node_factory.h>
#include <parser/parsenodes.h>
#include "function_builder.h"

namespace tpl::compiler {
using Block = ast::AstNode;
using Type = ast::Expr;

class Value {

 public:
  Type &GetType() const {
    return type_;
  }

  virtual ast::Identifier GetIdentifier() const {
    ast::Identifier ident(name_.data());
    return ident;
  }

  Expr *GetExpr() const {
    return expr_;
  }

 private:
  friend class Function;
  std::string name_;
  Type type_;
  Expr *expr_;
};

class Constant : public Value {

  ast::Identifier GetIdentifier() const override {
    ast::Identifier ident(value_.data());
    return ident;
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
    SourcePosition dummy;
    auto ret = nodeFactory->NewAssignmentStmt(dummy, nodeFactory->NewIdentifierExpr(dummy, assignee->GetIdentifier()),
        nodeFactory->NewIdentifierExpr(dummy, val->GetIdentifier()));
    blocks_.push_back(ret);
    return ret;
  }

  Block *Call(const Function *fn, std::initializer_list<Value *> arguments) {
    util::RegionVector<Expr *> args(region_.get());
    for (auto a : arguments) {
      args.push_back(a->GetExpr());
    }
    nodeFactory->NewCallExpr(fn->fnDecl, std::move(args));
  }

  Block Compile() {

  }

 private:
  std::shared_ptr<util::Region> region_;
  std::unique_ptr<ast::AstNodeFactory> nodeFactory;
  std::string name_;
  std::vector<Value *> params_;
  ast::FunctionDecl *fnDecl;
  util::RegionVector<ast::Stmt*> blocks_;
};

}