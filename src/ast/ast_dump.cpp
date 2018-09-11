#include "ast/ast_dump.h"

#include <iostream>

#include "llvm/Support/raw_os_ostream.h"

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "ast/type.h"

namespace tpl::ast {

class AstDumperImpl : public AstVisitor<AstDumperImpl> {
 public:
  explicit AstDumperImpl(AstNode *root, int out_fd)
      : root_(root),
        top_level_(true),
        first_child_(true),
        out_(out_fd, false) {}

  void Run() { Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 private:
  struct WithColor {
    AstDumperImpl &impl;
    WithColor(AstDumperImpl &impl, llvm::raw_ostream::Colors color)
        : impl(impl) {
      impl.out_.changeColor(color);
    }
    ~WithColor() { impl.out_.resetColor(); }
  };

  void DumpKind(ast::AstNode *node) {
    WithColor color(*this, llvm::raw_ostream::CYAN);
    out_ << " " << node->kind_name();
  }

  void DumpPointer(const void *p) {
    WithColor color(*this, llvm::raw_ostream::YELLOW);
    out_ << " (" << p << ")";
  }

  void DumpType(Type *type) {
    WithColor color(*this, llvm::raw_ostream::GREEN);
    out_ << " '" << Type::GetAsString(type) << "'";
  }

  void DumpPosition(const SourcePosition &pos) {
    out_ << " <";
    {
      WithColor color(*this, llvm::raw_ostream::YELLOW);
      out_ << "line:" << pos.line << ":" << pos.column;
    }
    out_ << ">";
  }

  void DumpNodeCommon(ast::AstNode *node) {
    DumpKind(node);
    DumpPointer(node);
    DumpPosition(node->position());
    out_ << " ";
  }

  void DumpToken(parsing::Token::Type type) {
    out_ << " '" << parsing::Token::String(type) << "'";
  }

  void DumpString(const std::string &str) { out_ << str; }

  void DumpIdentifier(Identifier str) { out_.write(str.data(), str.length()); }

  template <typename Fn>
  void DumpChild(Fn dump_fn) {
    if (top_level_) {
      top_level_ = false;
      dump_fn();
      while (!pending_.empty()) {
        pending_.pop_back_val()(true);
      }
      prefix_.clear();
      out_ << "\n";
      top_level_ = true;
    }

    auto dump_with_prefix = [this, dump_fn](bool last_child) {
      // First, the prefix
      out_ << "\n";
      out_ << prefix_ << (last_child ? "`" : "|") << "-";
      prefix_.append(last_child ? " " : "|").append(" ");

      first_child_ = true;
      auto depth = pending_.size();

      dump_fn();

      while (depth < pending_.size()) {
        pending_.pop_back_val()(true);
      }

      prefix_.resize(prefix_.size() - 2);
    };

    if (first_child_) {
      pending_.emplace_back(dump_with_prefix);
    } else {
      pending_.back()(false);
      pending_.back() = std::move(dump_with_prefix);
    }
    first_child_ = false;
  }

  void DumpDecl(Decl *decl) {
    DumpChild([=] { AstVisitor<AstDumperImpl>::Visit(decl); });
  }

  void DumpExpr(Expr *expr) {
    DumpChild([=] { AstVisitor<AstDumperImpl>::Visit(expr); });
  }

  void DumpStmt(Stmt *stmt) {
    DumpChild([=] { AstVisitor<AstDumperImpl>::Visit(stmt); });
  }

 private:
  AstNode *root_;

  std::string prefix_;

  bool top_level_;
  bool first_child_;

  llvm::SmallVector<std::function<void(bool)>, 32> pending_;

  llvm::raw_fd_ostream out_;
};

void AstDumperImpl::VisitFile(File *node) {
  DumpChild([=] {
    for (auto *decl : node->declarations()) {
      DumpDecl(decl);
    }
  });
}

void AstDumperImpl::VisitFieldDecl(FieldDecl *node) {
  DumpIdentifier(node->name());
  DumpString(":");
  Visit(node->type_repr());
}

void AstDumperImpl::VisitFunctionDecl(FunctionDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  DumpType(node->type_repr()->type());
  DumpExpr(node->function());
}

void AstDumperImpl::VisitVariableDecl(VariableDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  if (node->initial() != nullptr) {
    DumpExpr(node->initial());
  }
}

void AstDumperImpl::VisitStructDecl(StructDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  for (auto *field : node->type_repr()->fields()) {
    DumpDecl(field);
  }
}

void AstDumperImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->destination());
  DumpExpr(node->source());
}

void AstDumperImpl::VisitBlockStmt(BlockStmt *node) {
  DumpNodeCommon(node);
  for (auto *stmt : node->statements()) {
    DumpStmt(stmt);
  }
}

void AstDumperImpl::VisitDeclStmt(DeclStmt *node) {
  DumpDecl(node->declaration());
}

void AstDumperImpl::VisitExpressionStmt(ExpressionStmt *node) {
  DumpExpr(node->expression());
}

void AstDumperImpl::VisitForStmt(ForStmt *node) {
  DumpNodeCommon(node);
  if (node->init() != nullptr) {
    DumpStmt(node->init());
  }
  if (node->cond() != nullptr) {
    DumpExpr(node->cond());
  }
  if (node->next() != nullptr) {
    DumpStmt(node->next());
  }
  DumpStmt(node->body());
}

void AstDumperImpl::VisitIfStmt(IfStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->cond());
  DumpStmt(node->then_stmt());
  if (node->else_stmt() != nullptr) {
    DumpStmt(node->else_stmt());
  }
}

void AstDumperImpl::VisitReturnStmt(ReturnStmt *node) {
  DumpNodeCommon(node);
  if (node->ret() != nullptr) {
    DumpExpr(node->ret());
  }
}

void AstDumperImpl::VisitCallExpr(CallExpr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->function());
  for (auto *expr : node->arguments()) {
    DumpExpr(expr);
  }
}

void AstDumperImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  DumpNodeCommon(node);
  DumpToken(node->op());
  DumpExpr(node->left());
  DumpExpr(node->right());
}

void AstDumperImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  Visit(node->body());
}

void AstDumperImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
}

void AstDumperImpl::VisitLitExpr(LitExpr *node) {
  DumpNodeCommon(node);
  switch (node->literal_kind()) {
    case LitExpr::LitKind::Nil: {
      DumpString("nil");
      break;
    }
    case LitExpr::LitKind::Boolean: {
      DumpString(node->bool_val() ? "'true'" : "'false'");
      break;
    }
    case LitExpr::LitKind::Int:
    case LitExpr::LitKind::Float:
    case LitExpr::LitKind::String: {
      DumpIdentifier(node->raw_string());
      break;
    }
  }
}

void AstDumperImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  DumpNodeCommon(node);
  DumpToken(node->op());
  DumpExpr(node->expr());
}

void AstDumperImpl::VisitBadStmt(BadStmt *node) {
  DumpNodeCommon(node);
  DumpString("BAD STATEMENT @ ");
  DumpPosition(node->position());
}

void AstDumperImpl::VisitBadExpr(BadExpr *node) {
  DumpNodeCommon(node);
  DumpString("BAD EXPRESSION @ ");
  DumpPosition(node->position());
}

void AstDumperImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  DumpType(node->type());
}

void AstDumperImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  DumpType(node->type());
}

void AstDumperImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  DumpType(node->type());
}

void AstDumperImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  DumpType(node->type());
}

void AstDump::Dump(ast::AstNode *node) {
  AstDumperImpl print(node, fileno(stderr));
  print.Run();
}

}  // namespace tpl::ast