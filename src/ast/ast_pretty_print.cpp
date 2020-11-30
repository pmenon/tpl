#include "ast/ast_pretty_print.h"

#include <iostream>

#include "ast/ast_visitor.h"

namespace tpl::ast {

namespace {

class AstPrettyPrintImpl : public AstVisitor<AstPrettyPrintImpl> {
 public:
  explicit AstPrettyPrintImpl(std::ostream &os, AstNode *root) : os_(os), root_(root), indent_(0) {}

  void Run() { Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 private:
  void IncreaseIndent() { indent_ += 4; }

  void DecreaseIndent() { indent_ -= 4; }

  void NewLine() {
    os_ << std::endl;
    for (int32_t i = 0; i < indent_; i++) {
      os_ << " ";
    }
  }

 private:
  // The output stream.
  std::ostream &os_;
  // The AST.
  AstNode *root_;
  // Current indent level.
  int32_t indent_;
};

void AstPrettyPrintImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  os_ << "[";
  if (node->HasLength()) {
    Visit(node->GetLength());
  } else {
    os_ << "*";
  }
  os_ << "]";
  Visit(node->GetElementType());
}

void AstPrettyPrintImpl::VisitBadExpr(BadExpr *node) { TPL_ASSERT(false, "Invalid"); }

void AstPrettyPrintImpl::VisitBlockStmt(BlockStmt *node) {
  if (node->IsEmpty()) {
    os_ << "{ }";
    return;
  }

  os_ << "{";
  IncreaseIndent();
  NewLine();

  bool first = true;
  for (auto *stmt : node->GetStatements()) {
    if (!first) NewLine();
    first = false;
    Visit(stmt);
  }

  DecreaseIndent();
  NewLine();
  os_ << "}";
}

void AstPrettyPrintImpl::VisitCallExpr(CallExpr *node) {
  if (node->IsBuiltinCall()) os_ << "@";
  Visit(node->GetFunction());
  os_ << "(";
  bool first = true;
  for (auto *arg : node->GetArguments()) {
    if (!first) os_ << ", ";
    first = false;
    Visit(arg);
  }
  os_ << ")";
}

void AstPrettyPrintImpl::VisitFieldDeclaration(FieldDeclaration *node) {
  os_ << node->GetName().GetView() << ": ";
  Visit(node->GetTypeRepr());
}

void AstPrettyPrintImpl::VisitFunctionDeclaration(FunctionDeclaration *node) {
  os_ << "fun " << node->GetName().GetView();
  Visit(node->GetFunctionLiteral());
  NewLine();
}

void AstPrettyPrintImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  os_ << node->GetName().GetView();
}

void AstPrettyPrintImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  os_ << CastKindToString(node->GetCastKind()) << "(";
  Visit(node->GetInput());
  os_ << ")";
}

void AstPrettyPrintImpl::VisitReturnStmt(ReturnStmt *node) {
  os_ << "return";
  if (node->GetReturnValue() != nullptr) {
    os_ << " ";
    Visit(node->GetReturnValue());
  }
}

void AstPrettyPrintImpl::VisitStructDeclaration(StructDeclaration *node) {
  os_ << "struct " << node->GetName().GetView() << " {";
  IncreaseIndent();
  NewLine();
  Visit(node->GetTypeRepr());
  DecreaseIndent();
  NewLine();
  os_ << "}";
  NewLine();
}

void AstPrettyPrintImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  os_ << parsing::Token::GetString(node->Op());
  Visit(node->GetInput());
}

void AstPrettyPrintImpl::VisitVariableDeclaration(VariableDeclaration *node) {
  os_ << "var " << node->GetName().GetView();
  if (node->GetTypeRepr() != nullptr) {
    os_ << ": ";
    Visit(node->GetTypeRepr());
  }
  if (node->GetInitialValue() != nullptr) {
    os_ << " = ";
    Visit(node->GetInitialValue());
  }
}

void AstPrettyPrintImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  Visit(node->GetDestination());
  os_ << " = ";
  Visit(node->GetSource());
}

void AstPrettyPrintImpl::VisitFile(File *node) {
  for (auto *decl : node->GetDeclarations()) {
    Visit(decl);
  }
}

void AstPrettyPrintImpl::VisitFunctionLiteralExpr(FunctionLiteralExpr *node) {
  Visit(node->GetTypeRepr());
  os_ << " ";
  Visit(node->GetBody());
  NewLine();
}

void AstPrettyPrintImpl::VisitForStmt(ForStmt *node) {
  os_ << "for (";
  if (node->GetInit() != nullptr || node->GetNext() != nullptr) {
    // Standard
    if (node->GetInit() != nullptr) Visit(node->GetInit());
    os_ << "; ";
    Visit(node->GetCondition());
    os_ << "; ";
    if (node->GetNext() != nullptr) Visit(node->GetNext());
  } else if (node->GetCondition() != nullptr) {
    // While
    Visit(node->GetCondition());
  } else {
    // Unconditional loop
  }
  os_ << ") ";
  Visit(node->GetBody());
}

void AstPrettyPrintImpl::VisitForInStmt(ForInStmt *node) {
  os_ << "for (";
  Visit(node->Target());
  os_ << " in ";
  Visit(node->Iterable());
  os_ << ")";
  Visit(node->GetBody());
}

void AstPrettyPrintImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  Visit(node->GetLeft());
  os_ << " " << parsing::Token::GetString(node->Op()) << " ";
  Visit(node->GetRight());
}

void AstPrettyPrintImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  os_ << "map[";
  Visit(node->GetKeyType());
  os_ << "]";
  Visit(node->GetValueType());
}

void AstPrettyPrintImpl::VisitLiteralExpr(LiteralExpr *node) {
  switch (node->GetLiteralKind()) {
    case LiteralExpr::LiteralKind::Nil:
      os_ << "nil";
      break;
    case LiteralExpr::LiteralKind::Boolean:
      os_ << (node->BoolVal() ? "true" : "false");
      break;
    case LiteralExpr::LiteralKind::Int:
      os_ << node->IntegerVal();
      break;
    case LiteralExpr::LiteralKind::Float:
      os_ << node->FloatVal();
      break;
    case LiteralExpr::LiteralKind::String:
      os_ << "\"" << node->StringVal().GetView() << "\"";
      break;
  }
}

void AstPrettyPrintImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  // We want to ensure all types are aligned. Pre-process the fields to
  // find longest field names, then align as appropriate.

  std::size_t longest_field_len = 0;
  for (const auto *field : node->GetFields()) {
    longest_field_len = std::max(longest_field_len, field->GetName().GetLength());
  }

  bool first = true;
  for (const auto *field : node->GetFields()) {
    if (!first) NewLine();
    first = false;
    os_ << field->GetName().GetView();
    const std::size_t padding = longest_field_len - field->GetName().GetLength();
    os_ << std::string(padding, ' ') << ": ";
    Visit(field->GetTypeRepr());
  }
}

void AstPrettyPrintImpl::VisitDeclStmt(DeclStmt *node) { Visit(node->GetDeclaration()); }

void AstPrettyPrintImpl::VisitMemberExpr(MemberExpr *node) {
  Visit(node->GetObject());
  os_ << ".";
  Visit(node->GetMember());
}

void AstPrettyPrintImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  os_ << "*";
  Visit(node->GetBase());
}

void AstPrettyPrintImpl::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  Visit(node->GetLeft());
  os_ << " " << parsing::Token::GetString(node->Op()) << " ";
  Visit(node->GetRight());
}

void AstPrettyPrintImpl::VisitIfStmt(IfStmt *node) {
  os_ << "if (";
  Visit(node->GetCondition());
  os_ << ") ";
  Visit(node->GetThenStmt());
  if (node->GetElseStmt()) {
    os_ << " else ";
    Visit(node->GetElseStmt());
  }
}

void AstPrettyPrintImpl::VisitExpressionStmt(ExpressionStmt *node) { Visit(node->GetExpression()); }

void AstPrettyPrintImpl::VisitIndexExpr(IndexExpr *node) {
  Visit(node->GetObject());
  os_ << "[";
  Visit(node->GetIndex());
  os_ << "]";
}

void AstPrettyPrintImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  os_ << "(";
  bool first = true;
  for (const auto &param : node->GetParameters()) {
    if (!first) os_ << ", ";
    first = false;
    os_ << param->GetName().GetView() << ": ";
    Visit(param->GetTypeRepr());
  }
  os_ << ") -> ";
  Visit(node->GetReturnType());
}

}  // namespace

void AstPrettyPrint::Dump(std::ostream &os, AstNode *node) {
  AstPrettyPrintImpl printer(os, node);
  printer.Run();
  os << std::endl;
}

}  // namespace tpl::ast
