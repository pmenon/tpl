#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

void Sema::ReportIncorrectCallArg(ast::CallExpression *call, uint32_t index, ast::Type *expected) {
  error_reporter_->Report(call->Position(), ErrorMessages::kIncorrectCallArgType,
                          call->GetFuncName(), expected, index,
                          call->GetArguments()[index]->GetType());
}

void Sema::ReportIncorrectCallArg(ast::CallExpression *call, uint32_t index, const char *expected) {
  error_reporter_->Report(call->Position(), ErrorMessages::kIncorrectCallArgType2,
                          call->GetFuncName(), expected, index,
                          call->GetArguments()[index]->GetType());
}

ast::Expression *Sema::ImplCastExprToType(ast::Expression *expr, ast::Type *target_type,
                                          ast::CastKind cast_kind) {
  return context_->GetNodeFactory()->NewImplicitCastExpression(expr->Position(), cast_kind,
                                                               target_type, expr);
}

bool Sema::CheckArgCount(ast::CallExpression *call, uint32_t expected_arg_count) {
  if (call->NumArgs() != expected_arg_count) {
    error_reporter_->Report(call->Position(), ErrorMessages::kMismatchedCallArgs,
                            call->GetFuncName(), expected_arg_count, call->NumArgs());
    return false;
  }

  return true;
}

bool Sema::CheckArgCountAtLeast(ast::CallExpression *call, uint32_t expected_arg_count) {
  if (call->NumArgs() < expected_arg_count) {
    error_reporter_->Report(call->Position(), ErrorMessages::kMismatchedCallArgs,
                            call->GetFuncName(), expected_arg_count, call->NumArgs());
    return false;
  }

  return true;
}

// Logical ops: and, or only.
Sema::CheckResult Sema::CheckLogicalOperands(parsing::Token::Type op, const SourcePosition &pos,
                                             ast::Expression *left, ast::Expression *right) {
  // Both left and right types are either primitive booleans or SQL booleans. We
  // need both to be primitive booleans. Cast each expression as appropriate.

  ast::Type *const bool_type = ast::BuiltinType::Get(context_, ast::BuiltinType::Bool);

  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::BooleanVal)) {
    left = ImplCastExprToType(left, bool_type, ast::CastKind::SqlBoolToBool);
  }

  if (right->GetType()->IsSpecificBuiltin(ast::BuiltinType::BooleanVal)) {
    right = ImplCastExprToType(right, bool_type, ast::CastKind::SqlBoolToBool);
  }

  // If both input expressions are primitive booleans, we're done.
  if (left->GetType()->IsBoolType() && right->GetType()->IsBoolType()) {
    return {bool_type, left, right};
  }

  // Okay, there's an error ...
  error_reporter_->Report(pos, ErrorMessages::kMismatchedTypesToBinary, left->GetType(),
                          right->GetType(), op);

  return {nullptr, left, right};
}

// Arithmetic ops: +, -, *, etc.
Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op, const SourcePosition &pos,
                                                ast::Expression *left, ast::Expression *right) {
  // If neither inputs are arithmetic, fail early.
  if (!left->GetType()->IsArithmetic() || !right->GetType()->IsArithmetic()) {
    error_reporter_->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(),
                            right->GetType());
    return {nullptr, left, right};
  }

  // Arithmetic operators apply to both SQL and primitive numeric values and
  // yield a result of the same type as the first operand.

  if (left->GetType() == right->GetType()) {
    return {left->GetType(), left, right};
  }

  if (CheckAssignmentConstraints(left->GetType(), right)) {
    return {left->GetType(), left, right};
  }

  if (CheckAssignmentConstraints(right->GetType(), left)) {
    return {right->GetType(), left, right};
  }

  // Error.
  error_reporter_->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(),
                          right->GetType());

  return {nullptr, left, right};
}

// Comparisons: <, <=, >, >=, ==, !=
Sema::CheckResult Sema::CheckComparisonOperands(parsing::Token::Type op, const SourcePosition &pos,
                                                ast::Expression *left, ast::Expression *right) {
  TPL_ASSERT(parsing::Token::IsCompareOp(op), "Input operation token isn't a comparison.");
  TPL_ASSERT(left->GetType(), "Left input does not have a resolved type.");
  TPL_ASSERT(right->GetType(), "Right input does not have a resolved type.");

  // In any comparison, the first operand must be assignable to the type of the
  // second operand, or vice versa.
  //
  // The equality operators == and != apply to operands that are comparable.
  // The ordering operators <, <=, >, and >= apply to operands that are ordered.
  //
  // Rules:
  // - Primitive boolean values are comparable and ordered.
  // - Primitive integer values are comparable and ordered.
  // - Primitive floating point values are comparable and ordered.
  // - All SQL values are comparable and ordered.
  // - Pointer values are comparable.
  // - Struct values are comparable if all their fields are comparable. Two
  //   struct values are equal if their corresponding fields are equal.
  // - Array values are comparable if values of the array element type are also
  //   comparable. Two array values are equal if their corresponding elements
  //   are equal.

  // Check assignment constraints.
  if (!CheckAssignmentConstraints(right->GetType(), left)) {
    if (!CheckAssignmentConstraints(left->GetType(), right)) {
      error_reporter_->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(),
                              right->GetType());
      return {nullptr, left, right};
    }
  }

  // At this point, both operands must have the same type.
  TPL_ASSERT(left->GetType() == right->GetType(),
             "After operand assignment constraint checking, operands must have the same type.");

  // Pointers can only be used in equality-like comparisons.
  if (left->GetType()->IsPointerType() && !parsing::Token::IsEqualityOp(op)) {
    error_reporter_->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(),
                            right->GetType());
    return {nullptr, left, right};
  }

  // If the operands are SQL values, the result type is a SQL boolean.
  if (left->GetType()->IsSqlValueType()) {
    return {ast::BuiltinType::Get(context_, ast::BuiltinType::BooleanVal), left, right};
  }

  // Otherwise, the result type is a primitive bool.
  return {ast::BuiltinType::Get(context_, ast::BuiltinType::Bool), left, right};
}

bool Sema::CheckAssignmentConstraints(ast::Type *target_type, ast::Expression *&expr) {
  TPL_ASSERT(target_type != nullptr, "Target type cannot be null.");
  TPL_ASSERT(expr->GetType() != nullptr, "The input expression must have been type-checked.");

  // A value 'x' is assignable to a variable of type 'T' if one of the following
  // conditions are met:
  // 1. x's type is identical to T.
  // 2. x is the identifier nil and T is a pointer, function, or map type.
  // 3. x is a literal that is representable by a value of type T.
  // 4. x is a primitive boolean and T is a SQL boolean value.

  // If the expression's type matches the target type, nothing to do.
  if (expr->GetType() == target_type) {
    return true;
  }

  // Check assignment to a literal. This handles cases 2 and 3.
  if (auto literal = expr->SafeAs<ast::LiteralExpression>()) {
    if (literal->IsRepresentable(target_type)) {
      expr->SetType(target_type);
      return true;
    }
    return false;
  }

  // Convert *[N]Type to [*]Type.
  // This is so that we can pass around arrays to functions and force
  // users to take the address.
  if (auto target_arr = target_type->SafeAs<ast::ArrayType>()) {
    if (auto expr_base = expr->GetType()->GetPointeeType()) {
      if (auto expr_arr = expr_base->SafeAs<ast::ArrayType>()) {
        if (target_arr->HasUnknownLength() && expr_arr->HasKnownLength() &&
            target_arr->GetElementType() == expr_arr->GetElementType()) {
          expr = ImplCastExprToType(expr, target_type, ast::CastKind::BitCast);
          return true;
        }
      }
    }
  }

  // SQL bool to primitive bool.
  if (target_type->IsBoolType() &&
      expr->GetType()->IsSpecificBuiltin(ast::BuiltinType::BooleanVal)) {
    expr = ImplCastExprToType(expr, target_type, ast::CastKind::SqlBoolToBool);
    return true;
  }

  // Not a valid assignment.
  return false;
}

}  // namespace tpl::sema
