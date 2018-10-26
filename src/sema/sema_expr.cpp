#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/ast_node_factory.h"
#include "ast/type.h"
#include "logging/logger.h"

namespace tpl::sema {

void Sema::VisitBadExpr(ast::BadExpr *node) {
  TPL_ASSERT(false, "Bad expression in type checker!");
}

Sema::CheckResult Sema::CheckLogicalOperands(parsing::Token::Type op,
                                             const SourcePosition &pos,
                                             ast::Expr *left,
                                             ast::Expr *right) {
  // Are left and right types boolean values?
  if (left->type()->IsBoolType() && right->type()->IsBoolType()) {
    return {left->type(), left, right};
  }

  // Are left and right types SQL boolean values?
  if (left->type()->IsSqlType() && right->type()->IsSqlType()) {
    auto *left_type = left->type()->As<ast::SqlType>()->sql_type();
    auto *right_type = right->type()->As<ast::SqlType>()->sql_type();

    if (left_type->IsBoolean() && right_type->IsBoolean()) {
      sql::Type ret = *left_type;
      if (left_type->nullable() || right_type->nullable()) {
        ret = ret.AsNullable();
      }
      return {ast::SqlType::Get(ast_context(), ret), left, right};
    }
  }

  // We don't do any implicit casting for logical operators ...

  // Error
  error_reporter().Report(pos, ErrorMessages::kMismatchedTypesToBinary,
                          left->type(), right->type(), op);
  return {nullptr, left, right};
}

Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op,
                                                const SourcePosition &pos,
                                                ast::Expr *left,
                                                ast::Expr *right) {
  ast::Type *left_type = left->type();
  ast::Type *right_type = right->type();

  // Are left and right types arithmetic? If not, we early exit.
  if (!left_type->IsArithmetic() || !right_type->IsArithmetic()) {
    error_reporter().Report(pos, ErrorMessages::kIllegalTypesForBinary, op,
                            left_type, right_type);
    return {nullptr, left, right};
  }

  if (left_type == right_type) {
    return {left->type(), left, right};
  }

  if (!right_type->IsSqlType()) {
    // Implicitly cast the right to a SQL Integer
    right = ast_context().node_factory().NewImplicitCastExpr(
        right->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, right);

    sql::Type sql_type(sql::TypeId::Integer, false);
    right->set_type(ast::SqlType::Get(ast_context(), sql_type));
  }

  if (!left_type->IsSqlType()) {
    // Implicitly cast the left to a SQL Integer
    left = ast_context().node_factory().NewImplicitCastExpr(
        left->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, left);

    sql::Type sql_type(sql::TypeId::Integer, false);
    left->set_type(ast::SqlType::Get(ast_context(), sql_type));
  }

  sql::Type ret = *left_type->As<ast::SqlType>()->sql_type();
  if (left_type->As<ast::SqlType>()->sql_type()->nullable() ||
      right_type->As<ast::SqlType>()->sql_type()) {
    ret = ret.AsNullable();
  }
  return {ast::SqlType::Get(ast_context(), ret), left, right};
}

Sema::CheckResult Sema::CheckComparisonOperands(parsing::Token::Type op,
                                                const SourcePosition &pos,
                                                ast::Expr *left,
                                                ast::Expr *right) {
  ast::Type *left_type = left->type();
  ast::Type *right_type = right->type();

  // Are left and right types arithmetic? If not, we early exit.
  if (!left_type->IsArithmetic() || !right_type->IsArithmetic()) {
    error_reporter().Report(pos, ErrorMessages::kIllegalTypesForBinary, op,
                            left_type, right_type);
    return {nullptr, left, right};
  }

  if (left_type == right_type) {
    return {ast::BoolType::Get(ast_context()), left, right};
  }

  if (!right_type->IsSqlType()) {
    // Implicitly cast the right to a SQL Integer
    right = ast_context().node_factory().NewImplicitCastExpr(
        right->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, right);

    sql::Type sql_type(sql::TypeId::Integer, false);
    right->set_type(ast::SqlType::Get(ast_context(), sql_type));
  }

  if (!left_type->IsSqlType()) {
    // Implicitly cast the left to a SQL Integer
    left = ast_context().node_factory().NewImplicitCastExpr(
        left->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, left);

    sql::Type sql_type(sql::TypeId::Integer, false);
    left->set_type(ast::SqlType::Get(ast_context(), sql_type));
  }

  bool res_nullable = left_type->As<ast::SqlType>()->sql_type()->nullable() ||
                      right_type->As<ast::SqlType>()->sql_type();
  ast::SqlType *return_type = ast::SqlType::Get(
      ast_context(), sql::Type(sql::TypeId::Boolean, res_nullable));

  return {return_type, left, right};
}

void Sema::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  ast::Type *left_type = Resolve(node->left());
  ast::Type *right_type = Resolve(node->right());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      auto [result_type, left, right] = CheckLogicalOperands(
          node->op(), node->position(), node->left(), node->right());
      node->set_type(result_type);
      if (node->left() != left) node->set_left(left);
      if (node->right() != right) node->set_right(right);
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
      auto [result_type, left, right] = CheckArithmeticOperands(
          node->op(), node->position(), node->left(), node->right());
      node->set_type(result_type);
      if (node->left() != left) node->set_left(left);
      if (node->right() != right) node->set_right(right);
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

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::BANG_EQUAL:
    case parsing::Token::Type::EQUAL_EQUAL:
    case parsing::Token::Type::GREATER:
    case parsing::Token::Type::GREATER_EQUAL:
    case parsing::Token::Type::LESS:
    case parsing::Token::Type::LESS_EQUAL: {
      auto [result_type, left, right] = CheckComparisonOperands(
          node->op(), node->position(), node->left(), node->right());
      node->set_type(result_type);
      if (node->left() != left) node->set_left(left);
      if (node->right() != right) node->set_right(right);
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
    // Some error occurred
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
  if (auto *func_type = Resolve(node->type_repr()); func_type == nullptr) {
    // Some error occurred
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

void Sema::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  auto *expr_type = Resolve(node->input());
  (void)expr_type;
  // TODO: Check if the resolved input type can be casted to the target type
}

void Sema::VisitLitExpr(ast::LitExpr *node) {
  switch (node->literal_kind()) {
    case ast::LitExpr::LitKind::Nil: {
      node->set_type(ast::NilType::Get(ast_context()));
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      node->set_type(ast::BoolType::Get(ast_context()));
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      // Literal floats default to float32
      node->set_type(ast::FloatType::Get(ast_context(),
                                         ast::FloatType::FloatKind::Float32));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      // Literal integers default to int32
      node->set_type(ast::IntegerType::Get(ast_context(),
                                           ast::IntegerType::IntKind::Int32));
      break;
    }
    case ast::LitExpr::LitKind::String: {
      TPL_ASSERT(false, "String literals not supported yet");
      break;
    }
  }
}

void Sema::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->expr());

  if (expr_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::BANG: {
      if (!expr_type->IsBoolType()) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
        return;
      }

      node->set_type(expr_type);
      break;
    }
    case parsing::Token::Type::MINUS: {
      if (!expr_type->IsArithmetic()) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
        return;
      }

      node->set_type(expr_type);
      break;
    }
    case parsing::Token::Type::STAR: {
      if (!expr_type->IsPointerType()) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
        return;
      }

      node->set_type(expr_type->As<ast::PointerType>()->base());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      node->set_type(expr_type->PointerTo());
      break;
    }
    default: { UNREACHABLE("Impossible unary operation!"); }
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