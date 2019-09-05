#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"

namespace tpl::sema {

void Sema::ReportIncorrectCallArg(ast::CallExpr *call, u32 index, ast::Type *expected) {
  error_reporter()->Report(call->position(), ErrorMessages::kIncorrectCallArgType,
                           call->GetFuncName(), expected, index, call->arguments()[index]->type());
}

void Sema::ReportIncorrectCallArg(ast::CallExpr *call, u32 index, const char *expected) {
  error_reporter()->Report(call->position(), ErrorMessages::kIncorrectCallArgType2,
                           call->GetFuncName(), expected, index, call->arguments()[index]->type());
}

ast::Expr *Sema::ImplCastExprToType(ast::Expr *expr, ast::Type *target_type,
                                    ast::CastKind cast_kind) {
  return context()->node_factory()->NewImplicitCastExpr(expr->position(), cast_kind, target_type,
                                                        expr);
}

bool Sema::CheckArgCount(ast::CallExpr *call, u32 expected_arg_count) {
  if (call->num_args() != expected_arg_count) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs,
                             call->GetFuncName(), expected_arg_count, call->num_args());
    return false;
  }

  return true;
}

bool Sema::CheckArgCountAtLeast(ast::CallExpr *call, u32 expected_arg_count) {
  if (call->num_args() < expected_arg_count) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs,
                             call->GetFuncName(), expected_arg_count, call->num_args());
    return false;
  }

  return true;
}

// Logical ops: and, or
Sema::CheckResult Sema::CheckLogicalOperands(parsing::Token::Type op, const SourcePosition &pos,
                                             ast::Expr *left, ast::Expr *right) {
  //
  // Both left and right types are either primitive booleans or SQL booleans. We
  // need both to be primitive booleans. Cast each expression as appropriate.
  //

  ast::Type *const bool_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);

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

  error_reporter()->Report(pos, ErrorMessages::kMismatchedTypesToBinary, left->type(),
                           right->type(), op);

  return {nullptr, left, right};
}

// Arithmetic ops: +, -, *, etc.
Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op, const SourcePosition &pos,
                                                ast::Expr *left, ast::Expr *right) {
  // If neither inputs are arithmetic, fail early
  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(),
                             right->type());
    return {nullptr, left, right};
  }

  // If the inputs match up (and are arithmetic from the earlier check), finish.
  if (left->type() == right->type()) {
    return {left->type(), left, right};
  }

  // primitive int <OP> primitive int
  if (left->type()->IsIntegerType() && right->type()->IsIntegerType()) {
    if (left->type()->size() < right->type()->size()) {
      auto new_left = ImplCastExprToType(left, right->type(), ast::CastKind::IntegralCast);
      return {right->type(), new_left, right};
    }
    auto new_right = ImplCastExprToType(right, left->type(), ast::CastKind::IntegralCast);
    return {left->type(), left, new_right};
  }

  // primitive int <OP> SQL int
  if (left->type()->IsIntegerType() &&
      right->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    auto new_left = ImplCastExprToType(left, right->type(), ast::CastKind::IntToSqlInt);
    return {right->type(), new_left, right};
  }

  // SQL int <OP> primitive int
  if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer) &&
      right->type()->IsIntegerType()) {
    auto new_right = ImplCastExprToType(right, left->type(), ast::CastKind::IntToSqlInt);
    return {left->type(), left, new_right};
  }

  // primitive float <OP> SQL real
  if (left->type()->IsFloatType() && right->type()->IsSpecificBuiltin(ast::BuiltinType::Real)) {
    auto new_left = ImplCastExprToType(left, right->type(), ast::CastKind::FloatToSqlReal);
    return {right->type(), new_left, right};
  }

  // SQL real <OP> primitive float
  if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Real) && right->type()->IsFloatType()) {
    auto new_right = ImplCastExprToType(right, left->type(), ast::CastKind::FloatToSqlReal);
    return {left->type(), left, new_right};
  }

  // TODO(pmenon): Fix me to support other arithmetic types
  error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(),
                           right->type());
  return {nullptr, left, right};
}

// Comparisons: <, <=, >, >=, ==, !=
Sema::CheckResult Sema::CheckComparisonOperands(parsing::Token::Type op, const SourcePosition &pos,
                                                ast::Expr *left, ast::Expr *right) {
  if (left->type()->IsPointerType() || right->type()->IsPointerType()) {
    if (!parsing::Token::IsEqualityOp(op)) {
      error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(),
                               right->type());
      return {nullptr, left, right};
    }

    auto lhs = left->type()->GetPointeeType();
    auto rhs = right->type()->GetPointeeType();

    bool same_pointee_type = (lhs != nullptr && lhs == rhs);
    bool compare_with_nil = (lhs == nullptr && left->type()->IsNilType()) ||
                            (rhs == nullptr && right->type()->IsNilType());
    if (same_pointee_type || compare_with_nil) {
      auto *ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);
      return {ret_type, left, right};
    }

    // Error
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(),
                             right->type());
    return {nullptr, left, right};
  }

  // If neither input expression is arithmetic, it's an ill-formed operation
  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(),
                             right->type());
    return {nullptr, left, right};
  }

  auto built_ret_type = [this](ast::Type *input_type) {
    if (input_type->IsSpecificBuiltin(ast::BuiltinType::Integer) ||
        input_type->IsSpecificBuiltin(ast::BuiltinType::Real) ||
        input_type->IsSpecificBuiltin(ast::BuiltinType::Decimal)) {
      return ast::BuiltinType::Get(context(), ast::BuiltinType::Boolean);
    }
    return ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);
  };

  // If the input types are the same, we don't need to do any work
  if (left->type() == right->type()) {
    return {built_ret_type(left->type()), left, right};
  }

  // Cache a SQL integer type here because it's used throughout this function
  ast::Type *const sql_int_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Integer);

  // If either the left or right types aren't SQL integers, cast them up to one
  if (!right->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    right = ImplCastExprToType(right, sql_int_type, ast::CastKind::IntToSqlInt);
  }

  if (!left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    left = ImplCastExprToType(left, sql_int_type, ast::CastKind::IntToSqlInt);
  }

  // Done
  return {built_ret_type(left->type()), left, right};
}

bool Sema::CheckAssignmentConstraints(ast::Type *target_type, ast::Expr *&expr) {
  // If the target and expression types are the same, nothing to do
  if (expr->type() == target_type) {
    return true;
  }

  // Integer expansion
  if (target_type->IsIntegerType() && expr->type()->IsIntegerType()) {
    if (target_type->size() > expr->type()->size()) {
      expr = ImplCastExprToType(expr, target_type, ast::CastKind::IntegralCast);
    }
    return true;
  }

  // Float to integer expansion
  if (target_type->IsIntegerType() && expr->type()->IsFloatType()) {
    expr = ImplCastExprToType(expr, target_type, ast::CastKind::FloatToInt);
    return true;
  }

  // Integer to float expansion
  if (target_type->IsFloatType() && expr->type()->IsIntegerType()) {
    expr = ImplCastExprToType(expr, target_type, ast::CastKind::IntToFloat);
    return true;
  }

  // Convert *[N]Type to [*]Type
  if (auto *target_arr = target_type->SafeAs<ast::ArrayType>()) {
    if (auto *expr_base = expr->type()->GetPointeeType()) {
      if (auto *expr_arr = expr_base->SafeAs<ast::ArrayType>()) {
        if (target_arr->HasUnknownLength() && expr_arr->HasKnownLength()) {
          expr = ImplCastExprToType(expr, target_type, ast::CastKind::BitCast);
          return true;
        }
      }
    }
  }

  // *T to *U
  if (target_type->IsPointerType() || expr->type()->IsPointerType()) {
    expr = ImplCastExprToType(expr, target_type, ast::CastKind::BitCast);
    return true;
  }

  // SQL bool to primitive bool
  if (target_type->IsBoolType() && expr->type()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    expr = ImplCastExprToType(expr, target_type, ast::CastKind::SqlBoolToBool);
    return true;
  }

  // Not a valid assignment
  return false;
}

}  // namespace tpl::sema
