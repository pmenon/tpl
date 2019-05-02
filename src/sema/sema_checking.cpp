#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"

namespace tpl::sema {

ast::Expr *Sema::ImplCastExprToType(ast::Expr *expr, ast::Type *target_type,
                                    ast::CastKind cast_kind) {
  return context()->node_factory()->NewImplicitCastExpr(
      expr->position(), cast_kind, target_type, expr);
}

bool Sema::CheckArgCount(ast::CallExpr *call, u32 expected_arg_count) {
  if (call->num_args() != expected_arg_count) {
    error_reporter()->Report(
        call->position(), ErrorMessages::kMismatchedCallArgs,
        call->GetFuncName(), expected_arg_count, call->num_args());
    return true;
  }

  return false;
}

bool Sema::CheckArgCountAtLeast(ast::CallExpr *call, u32 expected_arg_count) {
  if (call->num_args() < expected_arg_count) {
    error_reporter()->Report(
        call->position(), ErrorMessages::kMismatchedCallArgs,
        call->GetFuncName(), expected_arg_count, call->num_args());
    return true;
  }

  return false;
}

// and, or
Sema::CheckResult Sema::CheckLogicalOperands(parsing::Token::Type op,
                                             const SourcePosition &pos,
                                             ast::Expr *left,
                                             ast::Expr *right) {
  //
  // Both left and right types are either primitive booleans or SQL booleans. We
  // need both to be primitive booleans. Cast each expression as appropriate.
  //

  ast::Type *const bool_type =
      ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);

  if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    left = ImplCastExprToType(left, bool_type, ast::CastKind::SqlBoolToBool);
  }

  if (right->type()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    right = ImplCastExprToType(right, bool_type, ast::CastKind::SqlBoolToBool);
  }

  // If both input expressions are primitive booleans, we're done
  if (left->type()->IsBoolType() && right->type()->IsBoolType()) {
    return {bool_type, left, right};
  }

  // Okay, there's an error ...

  error_reporter()->Report(pos, ErrorMessages::kMismatchedTypesToBinary,
                           left->type(), right->type(), op);

  return {nullptr, left, right};
}

// Check arithmetic operations: +, -, *, etc.
Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op,
                                                const SourcePosition &pos,
                                                ast::Expr *left,
                                                ast::Expr *right) {
  //
  // 1. If neither type is arithmetic, it's an error, report and quit.
  // 2. If the types are the same arithmetic, all good.
  // 3. Some casting is required ...
  //

  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op,
                             left->type(), right->type());
    return {nullptr, left, right};
  }

  if (left->type() == right->type()) {
    return {left->type(), left, right};
  }

  // TODO(pmenon): Fix me to support other arithmetic types

  ast::Type *const sql_int_type =
      ast::BuiltinType::Get(context(), ast::BuiltinType::Integer);

  if (!right->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    right = ImplCastExprToType(right, sql_int_type, ast::CastKind::IntToSqlInt);
  }
  if (!left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    left = ImplCastExprToType(left, sql_int_type, ast::CastKind::IntToSqlInt);
  }

  return {sql_int_type, left, right};
}

Sema::CheckResult Sema::CheckComparisonOperands(parsing::Token::Type op,
                                                const SourcePosition &pos,
                                                ast::Expr *left,
                                                ast::Expr *right) {
  // If neither input expression is arithmetic, it's an ill-formed operation
  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op,
                             left->type(), right->type());
    return {nullptr, left, right};
  }

  // If the input types are the same, we don't need to do any work
  if (left->type() == right->type()) {
    ast::Type *ret_type = nullptr;
    if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer) ||
        left->type()->IsSpecificBuiltin(ast::BuiltinType::Real) ||
        left->type()->IsSpecificBuiltin(ast::BuiltinType::Decimal)) {
      ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Boolean);
    } else {
      ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);
    }
    return {ret_type, left, right};
  }

  // Cache a SQL integer type here because it's used throughout this function
  ast::Type *const sql_int =
      ast::BuiltinType::Get(context(), ast::BuiltinType::Integer);

  // If either the left or right types aren't SQL integers, cast them up to one
  if (!right->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    right = ImplCastExprToType(right, sql_int, ast::CastKind::IntToSqlInt);
  }
  if (!left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    left = ImplCastExprToType(left, sql_int, ast::CastKind::IntToSqlInt);
  }

  // Done
  ast::Type *ret_type = nullptr;
  if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer) ||
      left->type()->IsSpecificBuiltin(ast::BuiltinType::Real) ||
      left->type()->IsSpecificBuiltin(ast::BuiltinType::Decimal)) {
    ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Boolean);
  } else {
    ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);
  }
  return {ret_type, left, right};
}

}  // namespace tpl::sema
