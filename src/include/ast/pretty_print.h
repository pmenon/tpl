#pragma once

#include <iostream>

#include "ast/ast_visitor.h"

namespace tpl::ast {

class PrettyPrint : public AstVisitor<PrettyPrint> {
 public:
  explicit PrettyPrint(AstNode *root) : root_(root) {}

  void Print() {
    Visit(root_);
    std::cout << result_ << std::endl;
  }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);

  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  GEN_VISIT_METHOD

 private:
  void BeginVisit() { result_.append("("); }

  void EndVisit() { result_.append(")"); }

  void PrintToken(parsing::Token::Type type) {
    result_.append(parsing::Token::String(type));
  }

  void PrintString(const std::string &str) { result_.append(str); }

  void PrintString(const AstString *str) {
    result_.append("'").append(str->bytes(), str->length()).append("'");
  }

  void NewLine() { result_.append("\n"); }

 private:
  AstNode *root_;

  std::string result_;
};

}  // namespace tpl::ast