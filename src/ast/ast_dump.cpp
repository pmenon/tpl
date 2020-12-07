#include "ast/ast_dump.h"

#include <string>
#include <utility>

#include "llvm/Support/raw_os_ostream.h"

#include "ast/ast.h"
#include "ast/ast_visitor.h"
#include "ast/type.h"

namespace tpl::ast {

class AstDumperImpl : public AstVisitor<AstDumperImpl> {
 public:
  explicit AstDumperImpl(std::ostream &os, AstNode *root)
      : root_(root), top_level_(true), first_child_(true), out_(os) {
    out_.enable_colors(true);
  }

  void Run() { Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 private:
  class WithColor {
   public:
    WithColor(AstDumperImpl *impl, llvm::raw_ostream::Colors color) : impl_(impl) {
      impl_->out_.changeColor(color);
    }
    ~WithColor() { impl_->out_.resetColor(); }

   private:
    AstDumperImpl *impl_;
  };

  void DumpKind(AstNode *node) {
    WithColor color(this, llvm::raw_ostream::CYAN);
    out_ << " " << node->KindName();
  }

  void DumpPointer(const void *p) {
    WithColor color(this, llvm::raw_ostream::YELLOW);
    out_ << " (" << p << ")";
  }

  void DumpType(Type *type) {
    WithColor color(this, llvm::raw_ostream::GREEN);
    out_ << " '" << Type::ToString(type) << "'";
  }

  void DumpPosition(const SourcePosition &pos) {
    out_ << " <";
    {
      WithColor color(this, llvm::raw_ostream::YELLOW);
      out_ << "line:" << pos.line << ":" << pos.column;
    }
    out_ << ">";
  }

  void DumpNodeCommon(AstNode *node) {
    DumpKind(node);
    DumpPointer(node);
    DumpPosition(node->Position());
    out_ << " ";
  }

  void DumpExpressionCommon(Expression *expr) {
    DumpNodeCommon(expr);
    if (expr->GetType() != nullptr) {
      DumpType(expr->GetType());
      out_ << " ";
    }
  }

  void DumpToken(parsing::Token::Type type) {
    out_ << "'" << parsing::Token::GetString(type) << "'";
  }

  template <typename T>
  void DumpPrimitive(const T &val) {
    out_ << val;
  }

  void DumpIdentifier(Identifier str) { out_ << str.GetView(); }

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
      {
        WithColor color(this, llvm::raw_ostream::BLUE);
        out_ << "\n";
        out_ << prefix_ << " " << (last_child ? "└" : "├") << "—";
        prefix_.append(last_child ? "  " : " |").append(" ");
      }

      first_child_ = true;
      auto depth = pending_.size();

      dump_fn();

      while (depth < pending_.size()) {
        pending_.pop_back_val()(true);
      }

      prefix_.resize(prefix_.size() - 3);
    };

    if (first_child_) {
      pending_.emplace_back(dump_with_prefix);
    } else {
      pending_.back()(false);
      pending_.back() = std::move(dump_with_prefix);
    }
    first_child_ = false;
  }

  void DumpDeclaration(Declaration *decl) {
    DumpChild([=, this] { AstVisitor<AstDumperImpl>::Visit(decl); });
  }

  void DumpExpression(Expression *expr) {
    DumpChild([=, this] { AstVisitor<AstDumperImpl>::Visit(expr); });
  }

  void DumpStatement(Statement *stmt) {
    DumpChild([=, this] { AstVisitor<AstDumperImpl>::Visit(stmt); });
  }

 private:
  // The root of the AST to dump.
  AstNode *root_;
  // The current prefix to use when printing an AST node.
  // This is adjusted as we traverse the tree to add/remove tab levels.
  std::string prefix_;

  bool top_level_;
  bool first_child_;

  // The list of pending outputs.
  llvm::SmallVector<std::function<void(bool)>, 32> pending_;

  // The stream to print the tree to.
  llvm::raw_os_ostream out_;
};

void AstDumperImpl::VisitFile(File *node) {
  DumpNodeCommon(node);
  DumpChild([=, this] {
    for (auto *decl : node->GetDeclarations()) {
      DumpDeclaration(decl);
    }
  });
}

void AstDumperImpl::VisitFieldDeclaration(FieldDeclaration *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->GetName());
  DumpExpression(node->GetTypeRepr());
}

void AstDumperImpl::VisitFunctionDeclaration(FunctionDeclaration *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->GetName());
  DumpExpression(node->GetFunctionLiteral());
}

void AstDumperImpl::VisitVariableDeclaration(VariableDeclaration *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->GetName());
  if (node->HasDeclaredType()) {
    DumpType(node->GetTypeRepr()->GetType());
  }
  if (node->HasInitialValue()) {
    DumpExpression(node->GetInitialValue());
  }
}

void AstDumperImpl::VisitStructDeclaration(StructDeclaration *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->GetName());
  for (auto *field : node->GetTypeRepr()->As<StructTypeRepr>()->GetFields()) {
    DumpDeclaration(field);
  }
}

void AstDumperImpl::VisitAssignmentStatement(AssignmentStatement *node) {
  DumpNodeCommon(node);
  DumpExpression(node->GetDestination());
  DumpExpression(node->GetSource());
}

void AstDumperImpl::VisitBlockStatement(BlockStatement *node) {
  DumpNodeCommon(node);
  for (auto *stmt : node->GetStatements()) {
    DumpStatement(stmt);
  }
}

void AstDumperImpl::VisitDeclarationStatement(DeclarationStatement *node) {
  AstVisitor<AstDumperImpl>::Visit(node->GetDeclaration());
}

void AstDumperImpl::VisitExpressionStatement(ExpressionStatement *node) {
  AstVisitor<AstDumperImpl>::Visit(node->GetExpression());
}

void AstDumperImpl::VisitForStatement(ForStatement *node) {
  DumpNodeCommon(node);
  if (node->GetInit() != nullptr) {
    DumpStatement(node->GetInit());
  }
  if (node->GetCondition() != nullptr) {
    DumpExpression(node->GetCondition());
  }
  if (node->GetNext() != nullptr) {
    DumpStatement(node->GetNext());
  }
  DumpStatement(node->GetBody());
}

void AstDumperImpl::VisitForInStatement(ForInStatement *node) {
  DumpNodeCommon(node);
  DumpExpression(node->Target());
  DumpExpression(node->Iterable());
  DumpStatement(node->GetBody());
}

void AstDumperImpl::VisitIfStatement(IfStatement *node) {
  DumpNodeCommon(node);
  DumpExpression(node->GetCondition());
  DumpStatement(node->GetThenStatement());
  if (node->HasElseStatement()) {
    DumpStatement(node->GetElseStatement());
  }
}

void AstDumperImpl::VisitReturnStatement(ReturnStatement *node) {
  DumpNodeCommon(node);
  if (node->GetReturnValue() != nullptr) {
    DumpExpression(node->GetReturnValue());
  }
}

void AstDumperImpl::VisitCallExpression(CallExpression *node) {
  DumpExpressionCommon(node);

  DumpPrimitive("<");
  {
    WithColor color(this, llvm::raw_ostream::Colors::RED);
    switch (node->GetCallKind()) {
      case CallExpression::CallKind::Builtin: {
        out_ << "Builtin";
        break;
      }
      case CallExpression::CallKind::Regular: {
        out_ << "Regular";
      }
    }
  }
  DumpPrimitive("> ");

  DumpExpression(node->GetFunction());
  for (auto *expr : node->GetArguments()) {
    DumpExpression(expr);
  }
}

void AstDumperImpl::VisitBinaryOpExpression(BinaryOpExpression *node) {
  DumpExpressionCommon(node);
  DumpToken(node->Op());
  DumpExpression(node->GetLeft());
  DumpExpression(node->GetRight());
}

void AstDumperImpl::VisitComparisonOpExpression(ComparisonOpExpression *node) {
  DumpExpressionCommon(node);
  DumpToken(node->Op());
  DumpExpression(node->GetLeft());
  DumpExpression(node->GetRight());
}

void AstDumperImpl::VisitFunctionLiteralExpression(FunctionLiteralExpression *node) {
  DumpExpressionCommon(node);
  DumpStatement(node->GetBody());
}

void AstDumperImpl::VisitIdentifierExpression(IdentifierExpression *node) {
  DumpExpressionCommon(node);
  DumpIdentifier(node->GetName());
}

void AstDumperImpl::VisitImplicitCastExpression(ImplicitCastExpression *node) {
  DumpExpressionCommon(node);
  DumpPrimitive("<");
  {
    WithColor color(this, llvm::raw_ostream::Colors::RED);
    DumpPrimitive(CastKindToString(node->GetCastKind()));
  }
  DumpPrimitive(">");
  DumpExpression(node->GetInput());
}

void AstDumperImpl::VisitIndexExpression(IndexExpression *node) {
  DumpExpressionCommon(node);
  DumpExpression(node->GetObject());
  DumpExpression(node->GetIndex());
}

void AstDumperImpl::VisitLiteralExpression(LiteralExpression *node) {
  DumpExpressionCommon(node);
  switch (node->GetLiteralKind()) {
    case LiteralExpression::LiteralKind::Nil:
      DumpPrimitive("nil");
      break;
    case LiteralExpression::LiteralKind::Boolean:
      DumpPrimitive(node->BoolVal() ? "'true'" : "'false'");
      break;
    case LiteralExpression::LiteralKind::Int:
      DumpPrimitive(node->IntegerVal());
      break;
    case LiteralExpression::LiteralKind::Float:
      DumpPrimitive(node->FloatVal());
      break;
    case LiteralExpression::LiteralKind::String:
      DumpIdentifier(node->StringVal());
      break;
  }
}

void AstDumperImpl::VisitMemberExpression(MemberExpression *node) {
  DumpExpressionCommon(node);
  DumpExpression(node->GetObject());
  DumpExpression(node->GetMember());
}

void AstDumperImpl::VisitUnaryOpExpression(UnaryOpExpression *node) {
  DumpExpressionCommon(node);
  DumpToken(node->Op());
  DumpExpression(node->GetInput());
}

void AstDumperImpl::VisitBadExpression(BadExpression *node) {
  DumpNodeCommon(node);
  DumpPrimitive("BAD EXPRESSION @ ");
  DumpPosition(node->Position());
}

void AstDumperImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  DumpNodeCommon(node);
  DumpType(node->GetType());
}

void AstDumperImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpression(node->GetBase());
}

void AstDumperImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  DumpNodeCommon(node);
  DumpType(node->GetType());
}

void AstDumperImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpression(node->GetLength());
  DumpExpression(node->GetElementType());
}

void AstDumperImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpression(node->GetKeyType());
  DumpExpression(node->GetValueType());
}

void AstDump::Dump(std::ostream &os, AstNode *node) {
  AstDumperImpl print(os, node);
  print.Run();
}

}  // namespace tpl::ast
