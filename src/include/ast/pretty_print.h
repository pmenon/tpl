#pragma once

#include <iostream>

#include "ast/ast_visitor.h"

namespace tpl {

class PrettyPrint : public AstVisitor<PrettyPrint> {
 public:
  explicit PrettyPrint(AstNode *root) : root_(root) {}

  void Print() {
    Visit(root_);
    std::cout << result_ << std::endl;
  }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(NodeType) void Visit##NodeType(NodeType *node);

  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  GEN_VISIT_METHOD

 private:
  void BeginVisit() { result_.append("("); }

  void EndVisit() { result_.append(")"); }

  void PrintToken(Token::Type type) { result_.append(Token::String(type)); }

  void PrintString(const std::string &str) {
    result_.append(str);
  }

  void PrintString(const AstString *str) {
    result_.append("'").append(str->bytes(), str->length()).append("'");
  }

 private:
  AstNode *root_;

  std::string result_;
};

}  // namespace tpl