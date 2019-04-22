#pragma once

#include <ast/ast.h>
#include <ast/ast_node_factory.h>
#include <parser/parsenodes.h>
#include <sstream>
#include "function_builder.h"

namespace tpl::compiler {
using Block = ast::AstNode;
using Type = ast::Expr;

class Value {
 public:
  Type &GetType() { return type_; }

  virtual ast::Identifier GetIdentifier() const {
    ast::Identifier ident(name_.data());
    return ident;
  }

  ast::Expr *GetExpr() { return expr_; }

 private:
  friend class Function;
  std::string name_;
  Type type_;
  ast::Expr *expr_;
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
  explicit Function(std::string name, std::vector<Value *> &&params,
                    Type *retType)
      : name_(name), blocks_(region_.get()) {
    params_ = std::move(params);
    nodeFactory = std::make_unique<ast::AstNodeFactory>(region_.get());
    ast::Identifier identifier(name_.data());
    // TODO(tanujnay112) sort this dummy business out
    SourcePosition dummy;
    util::RegionVector<ast::FieldDecl *> fields(region_.get());
    fields.reserve(4);
    for (auto v : params_) {
      ast::Identifier ident(v->name_.data());
      ast::Expr *type = &v->GetType();
      fields.push_back(nodeFactory->NewFieldDecl(dummy, ident, type));
    }
    ast::FunctionTypeRepr *fnType =
        nodeFactory->NewFunctionType(dummy, std::move(fields), retType);
    ast::Identifier fnName(name.data());
    fnDecl_ = nodeFactory->NewFunctionDecl(
        dummy, fnName, nodeFactory->NewFunctionLitExpr(fnType, nullptr));
  }

  Block *AssignValue(const Value *assignee, Value *val) {
    SourcePosition dummy;
    auto ret = nodeFactory->NewAssignmentStmt(
        dummy, nodeFactory->NewIdentifierExpr(dummy, assignee->GetIdentifier()),
        nodeFactory->NewIdentifierExpr(dummy, val->GetIdentifier()));
    blocks_.push_back(ret);
    return ret;
  }

  ast::Identifier GetIdentifier() {
    ast::Identifier ident(name_.data());
    return ident;
  }

  ast::IdentifierExpr *GetIdentifierExpr() {
    SourcePosition dummy;
    return nodeFactory->NewIdentifierExpr(dummy, GetIdentifier());
  }

  Block *Call(Function *fn, std::initializer_list<Value *> arguments) {
    util::RegionVector<ast::Expr *> args(region_.get());
    for (auto a : arguments) {
      args.push_back(a->GetExpr());
    }
    auto retBlock =
        nodeFactory->NewCallExpr(fn->GetIdentifierExpr(), std::move(args));
    blocks_.emplace_back(retBlock);
    return retBlock;
  }

  void Compile() {
    // compile block statement vector into one BlockStmt and put into fnDecl's
    // LitExpr
    SourcePosition dummy;
    ast::BlockStmt *blockStmt =
        nodeFactory->NewBlockStmt(dummy, dummy, std::move(blocks_));
    fnDecl_->function()->SetBody(blockStmt);
  }

 private:
  std::shared_ptr<util::Region> region_;
  std::unique_ptr<ast::AstNodeFactory> nodeFactory;
  std::string name_;
  std::vector<Value *> params_;
  ast::FunctionDecl *fnDecl_;
  util::RegionVector<ast::Stmt *> blocks_;
};

}  // namespace tpl::compiler