#include "sema/type_check.h"

namespace tpl::sema {

TypeChecker::TypeChecker(util::Region &region, ErrorReporter &error_reporter)
    : region_(region),
      error_reporter_(error_reporter),
      pointer_types_(region) {}

bool TypeChecker::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter().has_errors();
}

void TypeChecker::VisitBadExpression(ast::BadExpression *node) {}

void TypeChecker::VisitUnaryExpression(ast::UnaryExpression *node) {
  Visit(node->expr());

  ast::Type *expr_type = node->expr()->type();

  if (expr_type == nullptr) {
    return;
  }

  switch (node->op()) {
    case parsing::Token::BANG: {
      if (expr_type->IsBoolType()) {
        node->set_type(expr_type);
      } else {
        ReportError(node->position(), ErrorMessages::kInvalidOperation,
                    node->op(), expr_type->name());
      }
      break;
    }
    case parsing::Token::MINUS: {
      if (expr_type->IsNumber()) {
        node->set_type(expr_type);
      } else {
        ReportError(node->position(), ErrorMessages::kInvalidOperation,
                    node->op(), expr_type->name());
      }
      break;
    }
    case parsing::Token::Type::STAR: {
      if (auto *ptr_type = expr_type->SafeAs<ast::PointerType>()) {
        node->set_type(ptr_type->base());
      } else {
        ReportError(node->position(), ErrorMessages::kInvalidOperation,
                    node->op(), expr_type->name());
      }
      break;
    }
    default: {}
  }
}

void TypeChecker::VisitAssignmentStatement(ast::AssignmentStatement *node) {
  Visit(node->source());
  Visit(node->destination());
}

void TypeChecker::VisitBlockStatement(ast::BlockStatement *node) {
  Scope *block_scope = OpenScope(Scope::Kind::Block);

  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }

  CloseScope(block_scope);
}

void TypeChecker::VisitFile(ast::File *node) {
  Scope *file_scope = OpenScope(Scope::Kind::File);

  for (auto *decl : node->declarations()) {
    Visit(decl);
  }

  CloseScope(file_scope);
}

void TypeChecker::VisitVariableDeclaration(ast::VariableDeclaration *node) {
  if (scope()->LookupLocal(node->name()) != nullptr) {
    ReportError(node->position(), ErrorMessages::kVariableRedeclared,
                node->name());
    return;
  }

  ast::Type *declared_type = nullptr;
  ast::Type *initializer_type = nullptr;

  if (node->type_repr() != nullptr) {
    // Visit
    Visit(node->type_repr());

    // Pull out resolved type
    declared_type = node->type_repr()->type();
  }

  if (node->initial() != nullptr) {
    // Visit
    Visit(node->initial());

    // Pull out resolved type
    initializer_type = node->initial()->type();
  }

  if (declared_type != nullptr && initializer_type != nullptr) {
    // Check compatibility
  }

  // The type should be resolved now
  scope()->Declare(node->name(), (declared_type != nullptr ? declared_type
                                                           : initializer_type));
}

void TypeChecker::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {
  Visit(node->function());

  if (node->function()->type() == nullptr) {
    return;
  }

  scope()->Declare(node->name(), node->function()->type());
}

void TypeChecker::VisitStructDeclaration(ast::StructDeclaration *node) {}

void TypeChecker::VisitIdentifierExpression(ast::IdentifierExpression *node) {
  auto *type = scope()->Lookup(node->name());

  if (type == nullptr) {
    ReportError(node->position(), ErrorMessages::kUndefinedVariable,
                node->name());
    return;
  }

  node->set_type(type);
}

void TypeChecker::VisitCallExpression(ast::CallExpression *node) {
  // Resolve the function type
  Visit(node->function());

  ast::Type *type = node->function()->type();

  if (type == nullptr) {
    return;
  }

  if (!type->IsFunctionType()) {
    ReportError(node->position(), ErrorMessages::kNonFunction);
    return;
  }

  // Resolve each argument to the function
  auto *func_type = type->As<ast::FunctionType>();

  auto &param_types = func_type->params();

  auto &args = node->arguments();

  if (args.size() < param_types.size()) {
    ReportError(node->position(), ErrorMessages::kNotEnoughCallArgs);
    return;
  } else if (args.size() > param_types.size()) {
    ReportError(node->position(), ErrorMessages::kTooManyCallArgs);
    return;
  }

  for (size_t i = 0; i < args.size(); i++) {
    if (args[i]->type() != param_types[i]) {
      ReportError(node->position(), ErrorMessages::kIncorrectCallArgType);
      return;
    }
  }

  // All looks good ...
  node->set_type(func_type->return_type());
}

void TypeChecker::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {}

void TypeChecker::VisitLiteralExpression(ast::LiteralExpression *node) {}

void TypeChecker::VisitForStatement(ast::ForStatement *node) {}

void TypeChecker::VisitExpressionStatement(ast::ExpressionStatement *node) {
  Visit(node->expression());
}

void TypeChecker::VisitBadStatement(ast::BadStatement *node) {}

void TypeChecker::VisitStructTypeRepr(ast::StructTypeRepr *node) {}

void TypeChecker::VisitIfStatement(ast::IfStatement *node) {}

void TypeChecker::VisitDeclarationStatement(ast::DeclarationStatement *node) {
  Visit(node->declaration());
}

void TypeChecker::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {}

void TypeChecker::VisitBinaryExpression(ast::BinaryExpression *node) {}

void TypeChecker::VisitFunctionLiteralExpression(
    ast::FunctionLiteralExpression *node) {}

void TypeChecker::VisitReturnStatement(ast::ReturnStatement *node) {}

void TypeChecker::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {}

}  // namespace tpl::sema