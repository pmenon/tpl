#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"

namespace tpl::sema {

void Sema::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  auto *src_type = Resolve(node->source());
  auto *dest_type = Resolve(node->destination());

  if (src_type == nullptr || dest_type == nullptr) {
    // Skip
  }

  if (src_type != dest_type) {
    // Error
  }
}

void Sema::VisitBadExpr(ast::BadExpr *node) {
  TPL_ASSERT(false, "Bad expression in type checker!");
}

void Sema::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  ast::Type *left_type = Resolve(node->left());
  ast::Type *right_type = Resolve(node->right());

  switch (node->op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      if (!left_type->IsBoolType() || !right_type->IsBoolType()) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kMismatchedTypesToBinary,
                                left_type, right_type, node->op());
      }
      node->set_type(left_type);
      break;
    }
    case parsing::Token::Type::BANG_EQUAL:
    case parsing::Token::Type::EQUAL_EQUAL:
    case parsing::Token::Type::GREATER:
    case parsing::Token::Type::GREATER_EQUAL:
    case parsing::Token::Type::LESS:
    case parsing::Token::Type::LESS_EQUAL: {
      // Boolean comparison ops
      // TODO(pmenon): Check if compatible types
      node->set_type(ast::BoolType::Bool(left_type->context()));
      break;
    }
    default: {}
  }
}

void Sema::VisitCallExpr(ast::CallExpr *node) {
  // Resolve the function type
  ast::Type *type = Resolve(node->function());

  if (type == nullptr) {
    return;
  }

  if (!type->IsFunctionType()) {
    error_reporter().Report(node->position(), ErrorMessages::kNonFunction);
    return;
  }

  ast::Identifier func_name =
      node->function()->As<ast::IdentifierExpr>()->name();

  // First, check to make sure we have the right number of function arguments
  auto *func_type = type->As<ast::FunctionType>();
  if (node->arguments().size() < func_type->params().size()) {
    error_reporter().Report(node->position(), ErrorMessages::kNotEnoughCallArgs,
                            func_name);
    return;
  } else if (node->arguments().size() > func_type->params().size()) {
    error_reporter().Report(node->position(), ErrorMessages::kTooManyCallArgs,
                            func_name);
    return;
  }

  // Now, let's resolve each function argument's type
  for (auto *arg : node->arguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Now, let's make sure the arguments match up
  const auto &arg_types = node->arguments();
  const auto &func_param_types = func_type->params();
  for (size_t i = 0; i < arg_types.size(); i++) {
    if (arg_types[i]->type() != func_param_types[i]) {
      // TODO(pmenon): Fix this check
      error_reporter().Report(
          node->position(), ErrorMessages::kIncorrectCallArgType,
          arg_types[i]->type(), func_param_types[i], func_name);
      return;
    }
  }

  // All looks good ...
  node->set_type(func_type->return_type());
}

void Sema::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  // Resolve the type
  if (Resolve(node->type_repr()) == nullptr) {
    return;
  }

  // Good function type, insert into node
  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();
  node->set_type(func_type);

  // The function scope
  FunctionSemaScope function_scope(*this, node);

  // Declare function parameters in scope
  const auto &param_decls = node->type_repr()->parameters();
  const auto &param_types = func_type->params();
  for (size_t i = 0; i < func_type->params().size(); i++) {
    current_scope()->Declare(param_decls[i], param_types[i]);
  }

  // Recurse into the function body
  Visit(node->body());
}

void Sema::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  auto *type = current_scope()->Lookup(node->name());

  if (type == nullptr) {
    type = ast_context().LookupBuiltin(node->name());
    if (type == nullptr) {
      error_reporter().Report(node->position(),
                              ErrorMessages::kUndefinedVariable, node->name());
      return;
    }
  }

  node->set_type(type);
}

void Sema::VisitLitExpr(ast::LitExpr *node) {
  switch (node->literal_kind()) {
    case ast::LitExpr::LitKind::Nil: {
      node->set_type(ast::NilType::Nil(ast_context()));
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      node->set_type(ast::BoolType::Bool(ast_context()));
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      // Literal floats default to float32
      node->set_type(ast::FloatType::Float32(ast_context()));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      // Literal integers default to int32
      node->set_type(ast::IntegerType::Int32(ast_context()));
      break;
    }
    default: { TPL_ASSERT(false, "String literals not supported yet"); }
  }
}

void Sema::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->expr());

  if (expr_type == nullptr) {
    return;
  }

  switch (node->op()) {
    case parsing::Token::BANG: {
      if (expr_type->IsBoolType()) {
        node->set_type(expr_type);
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::MINUS: {
      if (expr_type->IsNumber()) {
        node->set_type(expr_type);
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::Type::STAR: {
      if (auto *ptr_type = expr_type->SafeAs<ast::PointerType>()) {
        node->set_type(ptr_type->base());
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      node->set_type(expr_type->PointerTo());
      break;
    }
    default: {}
  }
}

}  // namespace tpl::sema