#pragma once

#include <ast/ast.h>
#include <ast/ast_node_factory.h>
#include <parser/parsenodes.h>
#include <sstream>
#include "function_builder.h"

namespace tpl::compiler {
using Block = ast::AstNode;
using Type = ast::Expr;

class Function;

class Value {
 public:
  explicit Value(Type *t, std::string &&name) : name_(name), type_(t) {}

  Type *GetType() { return type_; }

  virtual ast::Identifier GetIdentifier() const {
    ast::Identifier ident(name_.data());
    return ident;
  }

  ast::Expr *GetExpr() { return type_; }

 private:
  friend class Function;
  std::string name_;
  Type *type_;
};

class Constant : public Value {
 public:
  explicit Constant(Type *t, std::string &&name, std::string &&value)
      : Value(t, std::move(name)), value_(value) {}

  ast::Identifier GetIdentifier() const override {
    ast::Identifier ident(value_.data());
    return ident;
  }

 private:
  friend class Function;
  std::string value_;
};

class CodeBlock {
 public:
  explicit CodeBlock(ast::AstNodeFactory *nodeFactory,
                     std::shared_ptr<util::Region> region)
      : region_(region), nodeFactory_(nodeFactory), blocks_(region_.get()) {}

  Block *AssignValue(const Value *assignee, Value *val) {
    SourcePosition dummy;
    auto ret = nodeFactory_->NewAssignmentStmt(
        dummy,
        nodeFactory_->NewIdentifierExpr(dummy, assignee->GetIdentifier()),
        nodeFactory_->NewIdentifierExpr(dummy, val->GetIdentifier()));
    blocks_.push_back(ret);
    return ret;
  }

  // TODO(tanujnay112) this doesn't support initializers yet
  Value *AllocateVariable(Type *type, std::string &name) {
    SourcePosition dummy;
    Value *variable = new Value(type, std::move(name));
    auto stmt = nodeFactory_->NewDeclStmt(nodeFactory_->NewVariableDecl(
        dummy, ast::Identifier(name.data()), type, nullptr));
    blocks_.push_back(stmt);
    return variable;
  }

  Block *CreateForInLoop(Value *target, Value *iter, CodeBlock *body, bool batch);

  Block *Call(Function *fn, std::initializer_list<Value *> arguments);

  ast::BlockStmt *Compile() {
    SourcePosition dummy;
    return nodeFactory_->NewBlockStmt(dummy, dummy, std::move(blocks_));
  }

 private:
  std::shared_ptr<util::Region> region_;
  std::unique_ptr<ast::AstNodeFactory> nodeFactory_;
  util::RegionVector<ast::Stmt *> blocks_;
};

class Function {
 public:
  explicit Function(ast::AstNodeFactory *nodeFactory, std::string name,
                    std::vector<Value *> &&params, Type *retType,
                    std::shared_ptr<util::Region> region)
      : nodeFactory_(nodeFactory), name_(name), body_(nodeFactory, region) {
    params_ = std::move(params);
    ast::Identifier identifier(name_.data());
    // TODO(tanujnay112) sort this dummy business out
    SourcePosition dummy;
    util::RegionVector<ast::FieldDecl *> fields(region.get());
    fields.reserve(4);
    for (auto v : params_) {
      ast::Identifier ident(v->name_.data());
      Type *type = v->GetType();
      fields.push_back(nodeFactory->NewFieldDecl(dummy, ident, type));
    }
    ast::FunctionTypeRepr *fnType =
        nodeFactory->NewFunctionType(dummy, std::move(fields), retType);
    ast::Identifier fnName(name.data());
    fnDecl_ = nodeFactory->NewFunctionDecl(
        dummy, fnName, nodeFactory->NewFunctionLitExpr(fnType, nullptr));
  }

  ast::Identifier GetIdentifier() {
    ast::Identifier ident(name_.data());
    return ident;
  }

  ast::IdentifierExpr *GetIdentifierExpr() {
    SourcePosition dummy;
    return nodeFactory_->NewIdentifierExpr(dummy, GetIdentifier());
  }

  std::string GetName() { return name_; }

  CodeBlock *GetCodeBlock() { return &body_; };

  void Compile() {
    // compile block statement vector into one BlockStmt and put into fnDecl's
    // LitExpr
    ast::BlockStmt *blockStmt = body_.Compile();
    fnDecl_->function()->SetBody(blockStmt);
  }

 private:
  ast::AstNodeFactory *nodeFactory_;
  std::string name_;
  std::vector<Value *> params_;
  ast::FunctionDecl *fnDecl_;
  CodeBlock body_;
};

}  // namespace tpl::compiler