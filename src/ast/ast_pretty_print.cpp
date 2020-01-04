#include "ast/ast_pretty_print.h"

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
  // The output stream
  std::ostream &os_;
  // The AST
  AstNode *root_;
  // Current indent level
  int32_t indent_;
};

void AstPrettyPrintImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  os_ << "[";
  if (node->HasLength()) {
    os_ << node->length();
  } else {
    os_ << "*";
  }
  os_ << "]";
  Visit(node->element_type());
}

void AstPrettyPrintImpl::VisitBadExpr(BadExpr *node) { TPL_ASSERT(false, "Invalid"); }

void AstPrettyPrintImpl::VisitBlockStmt(BlockStmt *node) {
  os_ << "{";
  IncreaseIndent();
  NewLine();

  bool first = true;
  for (auto *stmt : node->statements()) {
    if (!first) NewLine();
    first = false;
    Visit(stmt);
  }

  DecreaseIndent();
  NewLine();
  os_ << "}";
  NewLine();
}

void AstPrettyPrintImpl::VisitCallExpr(CallExpr *node) {
  Visit(node->function());
  os_ << "(";
  bool first = true;
  for (auto *arg : node->arguments()) {
    if (!first) os_ << ",";
    first = false;
    Visit(arg);
  };
  os_ << ")";
}

void AstPrettyPrintImpl::VisitFieldDecl(FieldDecl *node) {
  os_ << node->name().GetString() << ": ";
  Visit(node->type_repr());
}

void AstPrettyPrintImpl::VisitFunctionDecl(FunctionDecl *node) {
  os_ << "fun " << node->name().GetString();
  Visit(node->function());
}

void AstPrettyPrintImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  os_ << node->name().GetString();
}

void AstPrettyPrintImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  os_ << CastKindToString(node->cast_kind()) << "(";
  Visit(node->input());
  os_ << ")";
}

void AstPrettyPrintImpl::VisitReturnStmt(ReturnStmt *node) {
  os_ << "return";
  if (node->ret() != nullptr) {
    os_ << " ";
    Visit(node->ret());
  }
}

void AstPrettyPrintImpl::VisitStructDecl(StructDecl *node) {
  os_ << "struct " << node->name().GetString() << " {";
  IncreaseIndent();
  NewLine();
  Visit(node->type_repr());
  DecreaseIndent();
  os_ << "}";
  NewLine();
}

void AstPrettyPrintImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  os_ << parsing::Token::GetString(node->op());
  Visit(node->expr());
}

void AstPrettyPrintImpl::VisitVariableDecl(VariableDecl *node) {
  os_ << "var " << node->name().GetString();
  if (node->type_repr() != nullptr) {
    os_ << ": ";
    Visit(node->type_repr());
  }
  if (node->initial() != nullptr) {
    os_ << " = ";
    Visit(node->initial());
  }
}

void AstPrettyPrintImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  Visit(node->destination());
  os_ << " = ";
  Visit(node->source());
}

void AstPrettyPrintImpl::VisitFile(File *node) {
  for (auto *decl : node->GetDeclarations()) {
    Visit(decl);
  }
}

void AstPrettyPrintImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  Visit(node->type_repr());
  os_ << " ";
  Visit(node->body());
}

void AstPrettyPrintImpl::VisitForStmt(ForStmt *node) {
  os_ << "for (";
  if (node->init() != nullptr || node->next() != nullptr) {
    // Standard
    if (node->init() != nullptr) Visit(node->init());
    os_ << "; ";
    Visit(node->condition());
    os_ << "; ";
    if (node->next() != nullptr) Visit(node->next());
  } else if (node->condition() != nullptr) {
    // While
    Visit(node->condition());
  } else {
    // Unconditional loop
  }
  os_ << ") ";
  Visit(node->body());
}

void AstPrettyPrintImpl::VisitForInStmt(ForInStmt *node) {
  os_ << "for (";
  Visit(node->target());
  os_ << " in ";
  Visit(node->iter());
  os_ << ")";
  Visit(node->body());
}

void AstPrettyPrintImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  Visit(node->left());
  os_ << parsing::Token::GetString(node->op());
  Visit(node->right());
}

void AstPrettyPrintImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  os_ << "map[";
  Visit(node->key());
  os_ << "]";
  Visit(node->val());
}

void AstPrettyPrintImpl::VisitLitExpr(LitExpr *node) {
  switch (node->literal_kind()) {
    case LitExpr::LitKind::Nil:
      os_ << "nil";
      break;
    case LitExpr::LitKind::Boolean:
      os_ << node->bool_val();
      break;
    case LitExpr::LitKind::Int:
      os_ << node->int32_val();
      break;
    case LitExpr::LitKind::Float:
      os_ << node->float32_val();
      break;
    case LitExpr::LitKind::String:
      os_ << node->raw_string_val().GetString();
      break;
  }
}

void AstPrettyPrintImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  for (auto &field : node->fields()) {
    os_ << field->name().GetString() << ": ";
    Visit(field->type_repr());
    NewLine();
  }
}

void AstPrettyPrintImpl::VisitDeclStmt(DeclStmt *node) { Visit(node->declaration()); }

void AstPrettyPrintImpl::VisitMemberExpr(MemberExpr *node) {
  Visit(node->object());
  os_ << ".";
  Visit(node->member());
}

void AstPrettyPrintImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  os_ << "*";
  Visit(node->base());
}

void AstPrettyPrintImpl::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  Visit(node->left());
  os_ << " " << parsing::Token::GetString(node->op()) << " ";
  Visit(node->right());
}

void AstPrettyPrintImpl::VisitIfStmt(IfStmt *node) {
  os_ << "if (";
  Visit(node->condition());
  os_ << ") ";
  Visit(node->then_stmt());
  if (node->else_stmt()) {
    os_ << " else ";
    Visit(node->else_stmt());
  }
}

void AstPrettyPrintImpl::VisitExpressionStmt(ExpressionStmt *node) { Visit(node->expression()); }

void AstPrettyPrintImpl::VisitIndexExpr(IndexExpr *node) {
  Visit(node->object());
  os_ << "[";
  Visit(node->index());
  os_ << "]";
}

void AstPrettyPrintImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  os_ << "(";
  bool first = true;
  for (const auto &param : node->parameters()) {
    if (!first) os_ << ",";
    first = false;
    os_ << param->name().GetString() << ": ";
    Visit(param->type_repr());
  }
  os_ << ") -> ";
  Visit(node->return_type());
}

}  // namespace

void AstPrettyPrint::Dump(std::ostream &os, AstNode *node) {
  AstPrettyPrintImpl printer(os, node);
  printer.Run();
  os << std::endl;
}

}  // namespace tpl::ast
