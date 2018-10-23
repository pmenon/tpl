#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"
#include "logging/logger.h"

namespace tpl::sema {

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
    case parsing::Token::Type::AMPERSAND:
    case parsing::Token::Type::BIT_XOR:
    case parsing::Token::Type::BIT_OR:
    case parsing::Token::Type::PLUS:
    case parsing::Token::Type::MINUS:
    case parsing::Token::Type::STAR:
    case parsing::Token::Type::SLASH:
    case parsing::Token::Type::PERCENT: {
      // Arithmetic ops
      if (left_type != right_type) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kMismatchedTypesToBinary,
                                left_type, right_type, node->op());
        return;
      }
      node->set_type(left_type);
      break;
    }
    default: {
      LOG_ERROR("{} is not a binary operation!",
                parsing::Token::GetString(node->op()));
    }
  }
}

void Sema::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  ast::Type *left_type = Resolve(node->left());
  ast::Type *right_type = Resolve(node->right());

  // TODO(pmenon): Fix this check
  if (left_type != right_type) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kMismatchedTypesToBinary, left_type,
                            right_type, node->op());
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::BANG_EQUAL:
    case parsing::Token::Type::EQUAL_EQUAL:
    case parsing::Token::Type::GREATER:
    case parsing::Token::Type::GREATER_EQUAL:
    case parsing::Token::Type::LESS:
    case parsing::Token::Type::LESS_EQUAL: {
      node->set_type(ast::BoolType::Bool(left_type->context()));
      break;
    }
    default: {
      LOG_ERROR("{} is not a comparison operation",
                parsing::Token::GetString(node->op()));
    }
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
  const auto &func_params = func_type->params();
  for (size_t i = 0; i < arg_types.size(); i++) {
    if (arg_types[i]->type() != func_params[i].type) {
      error_reporter().Report(
          node->position(), ErrorMessages::kIncorrectCallArgType,
          arg_types[i]->type(), func_params[i].type, func_name);
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
  for (const auto &param : func_type->params()) {
    current_scope()->Declare(param.name, param.type);
  }

  // Recurse into the function body
  Visit(node->body());
}

void Sema::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Check the current context
  if (auto *type = current_scope()->Lookup(node->name())) {
    node->set_type(type);
    return;
  }

  // Check the builtins
  if (auto *type = ast_context().LookupBuiltin(node->name())) {
    node->set_type(type);
    return;
  }

  // Error
  error_reporter().Report(node->position(), ErrorMessages::kUndefinedVariable,
                          node->name());
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
    case parsing::Token::Type::BANG: {
      if (expr_type->IsBoolType()) {
        node->set_type(expr_type);
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::Type::MINUS: {
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

void Sema::VisitSelectorExpr(ast::SelectorExpr *node) {
  ast::Type *obj_type = Resolve(node->object());

  if (auto *pointer_type = obj_type->SafeAs<ast::PointerType>()) {
    // Unlike C/C++, TPL allows selectors on both struct types and pointers to
    // structs using the same '.' syntax.
    obj_type = pointer_type->base();
  }

  if (!obj_type->IsStructType()) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kSelObjectNotComposite, obj_type);
    return;
  }

  if (!node->selector()->IsIdentifierExpr()) {
    error_reporter().Report(node->selector()->position(),
                            ErrorMessages::kExpectedIdentifierForSelector);
    return;
  }

  ast::Identifier sel_name =
      node->selector()->As<ast::IdentifierExpr>()->name();

  ast::Type *field_type =
      obj_type->As<ast::StructType>()->LookupFieldByName(sel_name);

  if (field_type == nullptr) {
    error_reporter().Report(node->selector()->position(),
                            ErrorMessages::kFieldObjectDoesNotExist, sel_name,
                            obj_type);
    return;
  }

  node->set_type(field_type);
}

}  // namespace tpl::sema