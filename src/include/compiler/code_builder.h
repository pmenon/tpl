#pragma once

#include <ast/ast.h>
#include <ast/ast_node_factory.h>
#include <parser/parsenodes.h>
#include <sstream>
#include "function_builder.h"

namespace tpl::compiler {
using Block = ast::Stmt;
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

  virtual ast::Expr *GetIdentifierExpr(ast::AstNodeFactory *nodeFactory) const {
    SourcePosition dummy;
    return nodeFactory->NewIdentifierExpr(dummy, GetIdentifier());
  }

  ast::Expr *GetExpr() { return type_; }

  std::string &GetName() { return name_; };

 private:
  friend class Function;
  std::string name_;
  Type *type_;
};

template <class T>
class Constant : public Value {
 public:
  explicit Constant(Type *t, std::string &&name, T value)
      : Value(t, std::move(name)), value_(value) {
    TPL_ASSERT(GetType()->IsLitExpr(), "Not a literal type");
  }

  ast::Identifier GetIdentifier() const override {
    ast::Identifier ident(value_.data());
    return ident;
  }

  ast::Expr *GetIdentifierExpr(ast::AstNodeFactory *nodeFactory) const override;

 private:
  friend class Function;
  T value_;
};

class StructMember : public Value {
 public:
  explicit StructMember(Type *t, std::string &&name, ast::Identifier parent)
      : Value(t, std::move(name)) {
    hierarchy_.emplace_back(parent);
  }

  ast::Expr *GetIdentifierExpr(
      ast::AstNodeFactory *nodeFactory) const override {
    SourcePosition dummy;
    return nodeFactory->NewMemberExpr(
        dummy, nodeFactory->NewIdentifierExpr(dummy, hierarchy_.back()),
        nodeFactory->NewIdentifierExpr(dummy, GetIdentifier()));
  }

 private:
  std::vector<ast::Identifier> hierarchy_;
};

class StructVal : public Value {
 public:
  explicit StructVal(Type *t, std::string &&name) : Value(t, std::move(name)) {}

  StructMember *GetStructMember(std::string &&memberName) {
    return new StructMember(GetType(), std::move(memberName), GetIdentifier());
  }
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


  Block *CreateForInLoop(Value *target, Value *iter, CodeBlock *body,
                         bool batch);

  ast::BinaryOpExpr *ExecBinaryOp(Value *left, Value *right,
                                  parsing::Token::Type op) {
    SourcePosition dummy;
    return nodeFactory_->NewBinaryOpExpr(
        dummy, op, left->GetIdentifierExpr(nodeFactory_.get()),
        right->GetIdentifierExpr(nodeFactory_.get()));
  }

  ast::UnaryOpExpr *ExecUnaryOp(Value *target, parsing::Token::Type op) {
    SourcePosition dummy;
    return nodeFactory_->NewUnaryOpExpr(dummy, op, target->GetIdentifierExpr(nodeFactory_.get()));
  }

  Block *CreateIfStmt(ast::Expr *cond, CodeBlock *thenBlock, CodeBlock *elseBlock) {
    ast::Stmt *ret;
    SourcePosition dummy;
    if(elseBlock == nullptr) {
      ret = nodeFactory_->NewIfStmt(dummy, cond, thenBlock->Compile(), nullptr);
    } else {
      ret = nodeFactory_->NewIfStmt(dummy, cond, thenBlock->Compile(), elseBlock->Compile());
    }
    blocks_.emplace_back(ret);
    return ret;
  }

  Block *CreateForStmt(Block *init, ast::Expr *cond, Block *next, CodeBlock *body) {
    SourcePosition dummy;
    ast::Stmt *ret = nodeFactory_->NewForStmt(dummy, init, cond, next, body->Compile());
    blocks_.emplace_back(ret);
    return ret;
  }

  Block *BinaryOp(Value *dest, Value *left, Value *right, parsing::Token::Type op) {
    SourcePosition dummy;
    auto binOp = ExecBinaryOp(left, right, op);
    auto retBlock = nodeFactory_->NewAssignmentStmt(
        dummy, dest->GetIdentifierExpr(nodeFactory_.get()), binOp);
    blocks_.emplace_back(retBlock);
    return retBlock;
  }

  Block *OpAdd(Value *dest, Value *left, Value *right) {
    return BinaryOp(dest, left, right, parsing::Token::Type::PLUS);
  }

  ast::CallExpr *Call(Function *fn, std::initializer_list<Value *> arguments);

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