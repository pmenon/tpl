#include "ast/ast_dump.h"

#include <iostream>

#include "llvm/Support/raw_os_ostream.h"

#include "ast/ast.h"
#include "ast/ast_visitor.h"

namespace tpl::ast {

class AstDumperImpl : public AstVisitor<AstDumperImpl> {
 public:
  explicit AstDumperImpl(AstNode *root, int out_fd)
      : root_(root), indent_level_(0), stream_(out_fd, false) {}

  void Run() { Visit(root_); }

 private:
  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  // Generate primary visit method
  DEFINE_AST_VISITOR_METHOD()

  struct ScopedRecurse {
    AstDumperImpl &impl;
    ScopedRecurse(AstDumperImpl &impl) : impl(impl) {
      impl.IncrIndent();
      impl.NewLine();
    }
    ~ScopedRecurse() { impl.DecrIndent(); }
  };

  struct WithColor {
    AstDumperImpl &impl;
    WithColor(AstDumperImpl &impl, llvm::raw_ostream::Colors color)
        : impl(impl) {
      impl.stream_.changeColor(color);
    }
    ~WithColor() { impl.stream_.resetColor(); }
  };

  void IncrIndent() { indent_level_ += 2; }
  void DecrIndent() { indent_level_ -= 2; }

  void PrintIndent() { stream_.indent(indent_level_) << "-"; }

  void PrintPosition(const SourcePosition &pos) {
    stream_.changeColor(llvm::raw_ostream::WHITE) << "<";
    stream_.changeColor(llvm::raw_ostream::YELLOW)
        << "line:" << pos.line << ":" << pos.column;
    stream_.changeColor(llvm::raw_ostream::WHITE) << ">";
  }

  void PrintNodeCommon(ast::AstNode *node) {
    // Handle indent
    PrintIndent();

    // Kind
    stream_.changeColor(llvm::raw_ostream::Colors::CYAN);
    stream_ << node->kind_name() << " ";

    // Physical address
    stream_.changeColor(llvm::raw_ostream::Colors::MAGENTA) << "(";
    stream_ << "0x";
    stream_.write_hex(reinterpret_cast<uint64_t>(node)) << ")";

    //
    stream_ << " ";

    // Position
    PrintPosition(node->position());

    stream_ << " ";
  }

  void PrintToken(parsing::Token::Type type) {
    stream_ << "'" << parsing::Token::String(type) << "'";
  }

  void PrintString(const std::string &str) { stream_ << str; }

  void PrintIdentifier(Identifier str, bool highlight = false) {
    stream_.write(str.data(), str.length());
  }

  void NewLine() { stream_ << "\n"; }

 private:
  AstNode *root_;

  uint32_t indent_level_;

  llvm::raw_fd_ostream stream_;
};

void AstDumperImpl::VisitFile(File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
    NewLine();
  }
}

void AstDumperImpl::VisitFieldDecl(FieldDecl *node) {
  PrintIdentifier(node->name());
  PrintString(":");
  Visit(node->type_repr());
}

void AstDumperImpl::VisitFunctionDecl(FunctionDecl *node) {
  PrintNodeCommon(node);
  PrintIdentifier(node->name());
  PrintString(" '");
  Visit(node->type_repr());
  PrintString("' ");

  {
    ScopedRecurse recurse(*this);
    Visit(node->function());
  }
}

void AstDumperImpl::VisitVariableDecl(VariableDecl *node) {
  PrintNodeCommon(node);
  PrintIdentifier(node->name());
  // Visit(node->type_repr());
  if (node->initial() != nullptr) {
    ScopedRecurse recurse(*this);
    Visit(node->initial());
  }
}

void AstDumperImpl::VisitStructDecl(StructDecl *node) {
  PrintNodeCommon(node);
  PrintString("struct ");
  PrintIdentifier(node->name());
  PrintString("{ ");
  bool first = true;
  for (const auto *field : node->type_repr()->fields()) {
    if (!first) PrintString(",");
    first = false;
    PrintIdentifier(field->name());
    PrintString(":");
    Visit(field->type_repr());
  }
  PrintString("}");
}

void AstDumperImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  PrintNodeCommon(node);
  PrintString("=");

  ScopedRecurse recurse(*this);
  Visit(node->destination());
  NewLine();
  Visit(node->source());
}

void AstDumperImpl::VisitBlockStmt(BlockStmt *node) {
  PrintNodeCommon(node);

  ScopedRecurse recurse(*this);

  for (auto *statement : node->statements()) {
    Visit(statement);
    NewLine();
  }
}

void AstDumperImpl::VisitDeclStmt(DeclStmt *node) {
  Visit(node->declaration());
}

void AstDumperImpl::VisitExpressionStmt(ExpressionStmt *node) {
  Visit(node->expression());
}

void AstDumperImpl::VisitForStmt(ForStmt *node) {
  PrintNodeCommon(node);
  if (node->init() != nullptr) {
    ScopedRecurse recurse(*this);
    Visit(node->init());
  }
  if (node->cond() != nullptr) {
    ScopedRecurse recurse(*this);
    Visit(node->cond());
  }
  if (node->next() != nullptr) {
    ScopedRecurse recurse(*this);
    Visit(node->next());
  }
  ScopedRecurse recurse(*this);
  Visit(node->body());
}

void AstDumperImpl::VisitIfStmt(IfStmt *node) {
  PrintNodeCommon(node);

  {
    ScopedRecurse recurse(*this);
    Visit(node->cond());
    NewLine();
    Visit(node->then_stmt());
  }

  if (node->else_stmt() != nullptr) {
    ScopedRecurse recurse(*this);
    Visit(node->else_stmt());
  }
}

void AstDumperImpl::VisitReturnStmt(ReturnStmt *node) {
  PrintNodeCommon(node);
  if (node->ret() != nullptr) {
    ScopedRecurse recurse(*this);
    Visit(node->ret());
  }
}

void AstDumperImpl::VisitCallExpr(CallExpr *node) {
  PrintNodeCommon(node);
  {
    ScopedRecurse recurse(*this);
    Visit(node->function());
  }

  ScopedRecurse recurse(*this);

  for (auto *expr : node->arguments()) {
    Visit(expr);
    NewLine();
  }
}

void AstDumperImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  PrintNodeCommon(node);
  PrintToken(node->op());

  // Left expression
  {
    ScopedRecurse recurse_left(*this);
    Visit(node->left());
  }

  // Right expression
  {
    ScopedRecurse recurse_right(*this);
    Visit(node->right());
  }
}

void AstDumperImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  Visit(node->body());
}

void AstDumperImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  PrintNodeCommon(node);
  PrintIdentifier(node->name());
}

void AstDumperImpl::VisitLitExpr(LitExpr *node) {
  PrintNodeCommon(node);
  switch (node->literal_kind()) {
    case LitExpr::LitKind::Nil: {
      PrintString("nil");
      break;
    }
    case LitExpr::LitKind::Boolean: {
      PrintString(node->bool_val() ? "'true'" : "'false'");
      break;
    }
    case LitExpr::LitKind::Int:
    case LitExpr::LitKind::Float:
    case LitExpr::LitKind::String: {
      PrintIdentifier(node->raw_string());
      break;
    }
  }
}

void AstDumperImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  PrintNodeCommon(node);
  PrintToken(node->op());
  {
    ScopedRecurse recurse(*this);
    Visit(node->expr());
  }
}

void AstDumperImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  PrintString("struct {\n");
  for (auto *field : node->fields()) {
    Visit(field);
  }
  PrintString("}");
}

void AstDumperImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  PrintString("*");
  Visit(node->base());
}

void AstDumperImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  PrintString("(");
  bool first = true;
  for (auto *field : node->parameters()) {
    if (!first) PrintString(",");
    first = false;
    Visit(field);
  }
  PrintString(") -> ");
  Visit(node->return_type());
}

void AstDumperImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  PrintString("[");
  if (node->length() != nullptr) {
    Visit(node->length());
  }
  PrintString("]");
  Visit(node->element_type());
}

void AstDumperImpl::VisitBadStmt(BadStmt *node) {
  PrintNodeCommon(node);
  PrintString("BAD STATEMENT @ ");
  PrintPosition(node->position());
}

void AstDumperImpl::VisitBadExpr(BadExpr *node) {
  PrintNodeCommon(node);
  PrintString("BAD EXPRESSION @ ");
  PrintPosition(node->position());
}

void AstDump::Dump(ast::AstNode *node) {
  AstDumperImpl print(node, fileno(stderr));
  print.Run();
}

}  // namespace tpl::ast