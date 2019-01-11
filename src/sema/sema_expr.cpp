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
  /*
   * If the left expression is a SQL boolean value, cast it to a native boolean.
   * Do the same for the right expression.
   */

  if (auto *left_type = left->type()->SafeAs<ast::SqlType>();
      left_type != nullptr && left_type->sql_type().Is<sql::BooleanType>()) {
    // Implicit cast
    left = ast_context().node_factory().NewImplicitCastExpr(
        left->position(), ast::ImplicitCastExpr::CastKind::SqlBoolToBool, left);

    // Resolve
    left->set_type(ast::BoolType::Get(ast_context()));
  }

  if (auto *right_type = right->type()->SafeAs<ast::SqlType>();
      right_type != nullptr && right_type->sql_type().Is<sql::BooleanType>()) {
    // Implicit cast
    right = ast_context().node_factory().NewImplicitCastExpr(
        right->position(), ast::ImplicitCastExpr::CastKind::SqlBoolToBool,
        right);

    // Resolve
    right->set_type(ast::BoolType::Get(ast_context()));
  }

  /*
   * At this point, either left and right expressions are booleans, or there is
   * a semantic error.
   */

  if (left->type()->IsBoolType() && right->type()->IsBoolType()) {
    return {ast::BoolType::Get(ast_context()), left, right};
  }

  error_reporter().Report(pos, ErrorMessages::kMismatchedTypesToBinary,
                          left->type(), right->type(), op);
  return {nullptr, left, right};
}

Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op,
                                                const SourcePosition &pos,
                                                ast::Expr *left,
                                                ast::Expr *right) {
  /*
   * If neither the left expression or the right expression are arithmetic,
   * including arithmetic SQL types, there's a semantic error. Early exit.
   */

  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter().Report(pos, ErrorMessages::kIllegalTypesForBinary, op,
                            left->type(), right->type());
    return {nullptr, left, right};
  }

  /*
   * Both types are arithmetic. If they're the exact same type, we don't need to
   * do any additional work. Return.
   */

  if (left->type() == right->type()) {
    return {left->type(), left, right};
  }

  if (!right->type()->IsSqlType()) {
    // Implicitly cast the right to a SQL Integer
    right = ast_context().node_factory().NewImplicitCastExpr(
        right->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, right);

    right->set_type(
        ast::SqlType::Get(ast_context(), sql::IntegerType::Instance<false>()));
  }

  if (!left->type()->IsSqlType()) {
    // Implicitly cast the left to a SQL Integer
    left = ast_context().node_factory().NewImplicitCastExpr(
        left->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, left);

    left->set_type(
        ast::SqlType::Get(ast_context(), sql::IntegerType::Instance<false>()));
  }

  const sql::Type &ret = left->type()->As<ast::SqlType>()->sql_type();
  if (left->type()->As<ast::SqlType>()->sql_type().nullable() ||
      right->type()->As<ast::SqlType>()->sql_type().nullable()) {
    return {ast::SqlType::Get(ast_context(), ret.GetNullableVersion()), left,
            right};
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

    right_type =
        ast::SqlType::Get(ast_context(), sql::IntegerType::Instance(false));
    right->set_type(right_type);
  }

  if (!left_type->IsSqlType()) {
    // Implicitly cast the left to a SQL Integer
    left = ast_context().node_factory().NewImplicitCastExpr(
        left->position(), ast::ImplicitCastExpr::CastKind::IntToSqlInt, left);

    left_type =
        ast::SqlType::Get(ast_context(), sql::IntegerType::Instance(false));
    left->set_type(left_type);
  }

  bool res_nullable = left_type->As<ast::SqlType>()->sql_type().nullable() ||
                      right_type->As<ast::SqlType>()->sql_type().nullable();
  ast::SqlType *return_type = ast::SqlType::Get(
      ast_context(), sql::BooleanType::Instance(res_nullable));

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

void Sema::CheckBuiltinMapCall(ast::CallExpr *call) {}

void Sema::CheckBuiltinFilterCall(ast::CallExpr *call) {
  if (call->num_args() != 2) {
    auto func_name = call->function()->As<ast::IdentifierExpr>()->name();
    error_reporter().Report(call->position(),
                            ErrorMessages::kMismatchedCallArgs, func_name, 2,
                            call->num_args());
    return;
  }

  // Type-check arguments to filter
  ast::Type *first_arg_type = Resolve(call->arguments()[0]);
  ast::Type *second_arg_type = Resolve(call->arguments()[1]);

  if (first_arg_type == nullptr || second_arg_type == nullptr) {
    return;
  }

  // Check first is an array
  if (!first_arg_type->IsArrayType()) {
    error_reporter().Report(call->position(), ErrorMessages::kBadArgToFilter,
                            first_arg_type);
    return;
  }

  // Check second is a function
  if (!second_arg_type->IsFunctionType()) {
    error_reporter().Report(call->position(), ErrorMessages::kBadArgToFilter,
                            second_arg_type);
    return;
  }

  // Check input to function is the same as nested array element type
  auto arr_type = first_arg_type->As<ast::ArrayType>();
  auto func_type = second_arg_type->As<ast::FunctionType>();

  bool non_bool_return = !func_type->return_type()->IsBoolType();
  bool mismatched_input =
      func_type->num_params() != 1 ||
      func_type->params()[0].type != arr_type->element_type();

  if (non_bool_return || mismatched_input) {
    error_reporter().Report(call->position(), ErrorMessages::kBadFuncToFilter,
                            func_type->params()[0].type,
                            func_type->return_type(), arr_type->element_type());
    return;
  }

  // All good ...
  util::RegionVector<ast::Field> params(ast_context().region());
  params.emplace_back(ast_context().GetIdentifier("input"), arr_type);
  params.emplace_back(ast_context().GetIdentifier("lambda"), func_type);
  auto *filter_type = ast::FunctionType::Get(std::move(params), arr_type);

  call->function()->set_type(filter_type);
  call->set_type(filter_type->return_type());
}

void Sema::CheckBuiltinCall(ast::CallExpr *call, ast::Builtin builtin) {
  call->set_call_kind(ast::CallExpr::CallKind::Builtin);
  switch (builtin) {
    case ast::Builtin::Filter: {
      CheckBuiltinFilterCall(call);
      break;
    }
    case ast::Builtin::Map: {
      CheckBuiltinMapCall(call);
      break;
    }
    default: {
      // No-op
      break;
    }
  }
}

void Sema::VisitCallExpr(ast::CallExpr *node) {
  auto func_name = node->function()->As<ast::IdentifierExpr>()->name();

  // Is this a built in?
  if (ast::Builtin builtin;
      ast_context().IsBuiltinFunction(func_name, &builtin)) {
    CheckBuiltinCall(node, builtin);
    return;
  }

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

  // First, check to make sure we have the right number of function arguments
  auto *func_type = type->As<ast::FunctionType>();
  if (func_type->num_params() != node->num_args()) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kMismatchedCallArgs, func_name,
                            func_type->num_params(), node->num_args());
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
  node->set_call_kind(ast::CallExpr::CallKind::Regular);
  node->set_type(func_type->return_type());
}

void Sema::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  // Resolve the type
  if (auto *type = Resolve(node->type_repr()); type == nullptr) {
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

  // Check return
  if (!func_type->return_type()->IsNilType()) {
    if (node->IsEmpty() || !ast::Stmt::IsTerminating(node->body())) {
      error_reporter().Report(node->body()->right_brace_position(),
                              ErrorMessages::kMissingReturn);
      return;
    }
  }

  // If an empty and nil-return function, synthesize return
  if (node->IsEmpty() && func_type->return_type()->IsNilType()) {
    ast::ReturnStmt *empty_ret =
        ast_context().node_factory().NewReturnStmt(node->position(), nullptr);
    node->body()->statements().push_back(empty_ret);
  }
}

void Sema::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Check the current context
  if (auto *type = current_scope()->Lookup(node->name())) {
    node->set_type(type);
    return;
  }

  // Check the builtin types
  if (auto *type = ast_context().LookupBuiltinType(node->name())) {
    node->set_type(type);
    return;
  }

  // Error
  error_reporter().Report(node->position(), ErrorMessages::kUndefinedVariable,
                          node->name());
}

void Sema::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  ast::Type *expr_type = Resolve(node->input());

  if (expr_type == nullptr) {
    // Error
    return;
  }

  switch (node->cast_kind()) {
    case ast::ImplicitCastExpr::CastKind::IntToSqlInt: {
      if (!expr_type->IsIntegerType()) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidCastToSqlInt, expr_type);
        return;
      }

      // The type is a non-null SQL integer
      node->set_type(ast::SqlType::Get(expr_type->context(),
                                       sql::IntegerType::Instance<false>()));

      break;
    }
    case ast::ImplicitCastExpr::CastKind::IntToSqlDecimal: {
      if (!expr_type->IsIntegerType()) {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidCastToSqlDecimal,
                                expr_type);
        return;
      }

      // The type is a non-null SQL decimal
      node->set_type(ast::SqlType::Get(
          expr_type->context(), sql::DecimalType::Instance<false>(1, 2)));

      break;
    }
    case ast::ImplicitCastExpr::CastKind::SqlBoolToBool: {
      if (auto *type = expr_type->SafeAs<ast::SqlType>();
          type == nullptr || !type->sql_type().Is<sql::BooleanType>()) {
        error_reporter().Report(
            node->position(), ErrorMessages::kInvalidSqlCastToBool, expr_type);
        return;
      }

      // Type is native boolean
      node->set_type(ast::BoolType::Get(expr_type->context()));

      break;
    }
  }
}

void Sema::VisitIndexExpr(ast::IndexExpr *node) {
  ast::Type *obj_type = Resolve(node->object());
  ast::Type *index_type = Resolve(node->index());

  if (obj_type == nullptr || index_type == nullptr) {
    // Error
    return;
  }

  if (!obj_type->IsArrayType() && !obj_type->IsMapType()) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kInvalidIndexOperation, obj_type);
    return;
  }

  if (!index_type->IsIntegerType()) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kInvalidArrayIndexValue);
    return;
  }

  if (auto *arr_type = obj_type->SafeAs<ast::ArrayType>()) {
    node->set_type(arr_type->element_type());
  } else {
    node->set_type(obj_type->As<ast::MapType>()->value_type());
  }
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

void Sema::VisitMemberExpr(ast::MemberExpr *node) {
  ast::Type *obj_type = Resolve(node->object());

  if (obj_type == nullptr) {
    // Some error
    return;
  }

  if (auto *pointer_type = obj_type->SafeAs<ast::PointerType>()) {
    obj_type = pointer_type->base();
  }

  if (!obj_type->IsStructType()) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kSelObjectNotComposite, obj_type);
    return;
  }

  if (!node->member()->IsIdentifierExpr()) {
    error_reporter().Report(node->member()->position(),
                            ErrorMessages::kExpectedIdentifierForSelector);
    return;
  }

  ast::Identifier member = node->member()->As<ast::IdentifierExpr>()->name();

  ast::Type *member_type =
      obj_type->As<ast::StructType>()->LookupFieldByName(member);

  if (member_type == nullptr) {
    error_reporter().Report(node->member()->position(),
                            ErrorMessages::kFieldObjectDoesNotExist, member,
                            obj_type);
    return;
  }

  node->set_type(member_type);
}

}  // namespace tpl::sema