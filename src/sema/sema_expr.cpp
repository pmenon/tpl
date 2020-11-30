#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"
#include "logging/logger.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

void Sema::VisitBadExpression(ast::BadExpression *node) {
  TPL_ASSERT(false, "Bad expression in type checker!");
}

void Sema::VisitBinaryOpExpression(ast::BinaryOpExpression *node) {
  ast::Type *left_type = Resolve(node->GetLeft());
  ast::Type *right_type = Resolve(node->GetRight());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      auto [result_type, left, right] =
          CheckLogicalOperands(node->Op(), node->Position(), node->GetLeft(), node->GetRight());
      node->SetType(result_type);
      if (node->GetLeft() != left) node->SetLeft(left);
      if (node->GetRight() != right) node->SetRight(right);
      break;
    }
    case parsing::Token::Type::AMPERSAND:
    case parsing::Token::Type::BIT_XOR:
    case parsing::Token::Type::BIT_OR:
    case parsing::Token::Type::BIT_SHL:
    case parsing::Token::Type::BIT_SHR:
    case parsing::Token::Type::PLUS:
    case parsing::Token::Type::MINUS:
    case parsing::Token::Type::STAR:
    case parsing::Token::Type::SLASH:
    case parsing::Token::Type::PERCENT: {
      auto [result_type, left, right] =
          CheckArithmeticOperands(node->Op(), node->Position(), node->GetLeft(), node->GetRight());
      node->SetType(result_type);
      if (node->GetLeft() != left) node->SetLeft(left);
      if (node->GetRight() != right) node->SetRight(right);
      break;
    }
    default: {
      LOG_ERROR("{} is not a binary operation!", parsing::Token::GetString(node->Op()));
    }
  }
}

void Sema::VisitComparisonOpExpression(ast::ComparisonOpExpression *node) {
  ast::Type *left_type = Resolve(node->GetLeft());
  ast::Type *right_type = Resolve(node->GetRight());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::BANG_EQUAL:
    case parsing::Token::Type::EQUAL_EQUAL:
    case parsing::Token::Type::GREATER:
    case parsing::Token::Type::GREATER_EQUAL:
    case parsing::Token::Type::LESS:
    case parsing::Token::Type::LESS_EQUAL: {
      auto [result_type, left, right] =
          CheckComparisonOperands(node->Op(), node->Position(), node->GetLeft(), node->GetRight());
      node->SetType(result_type);
      if (node->GetLeft() != left) node->SetLeft(left);
      if (node->GetRight() != right) node->SetRight(right);
      break;
    }
    default: {
      LOG_ERROR("{} is not a comparison operation", parsing::Token::GetString(node->Op()));
    }
  }
}

void Sema::VisitCallExpression(ast::CallExpression *node) {
  // If the call claims to be to a builtin, validate it
  if (node->GetCallKind() == ast::CallExpression::CallKind::Builtin) {
    CheckBuiltinCall(node);
    return;
  }

  // Resolve the function type
  ast::Type *type = Resolve(node->GetFunction());
  if (type == nullptr) {
    return;
  }

  // Check that the resolved function type is actually a function
  auto *func_type = type->SafeAs<ast::FunctionType>();
  if (func_type == nullptr) {
    error_reporter_->Report(node->Position(), ErrorMessages::kNonFunction);
    return;
  }

  // Check argument count matches
  if (!CheckArgCount(node, func_type->GetNumParams())) {
    return;
  }

  // Resolve function arguments
  for (auto *arg : node->GetArguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Check args
  bool has_errors = false;

  const auto &actual_args = node->GetArguments();
  for (uint32_t arg_num = 0; arg_num < actual_args.size(); arg_num++) {
    ast::Type *expected_type = func_type->GetParams()[arg_num].type;
    ast::Expression *arg = actual_args[arg_num];

    // Function application simplifies to performing an assignment of the
    // actual call arguments to the function parameters. Do the check now, which
    // may apply an implicit cast to make the assignment work.
    if (!CheckAssignmentConstraints(expected_type, arg)) {
      has_errors = true;
      error_reporter_->Report(arg->Position(), ErrorMessages::kIncorrectCallArgType,
                              node->GetFuncName(), expected_type, arg_num, arg->GetType());
      continue;
    }

    // If the check applied an implicit cast, set the argument
    if (arg != actual_args[arg_num]) {
      node->SetArgument(arg_num, arg);
    }
  }

  if (has_errors) {
    return;
  }

  // Looks good ...
  node->SetType(func_type->GetReturnType());
}

void Sema::VisitFunctionLiteralExpression(ast::FunctionLiteralExpression *node) {
  // Resolve the type, if not resolved already
  if (auto *type = node->GetTypeRepr()->GetType(); type == nullptr) {
    type = Resolve(node->GetTypeRepr());
    if (type == nullptr) {
      return;
    }
  }

  // Good function type, insert into node
  auto *func_type = node->GetTypeRepr()->GetType()->As<ast::FunctionType>();
  node->SetType(func_type);

  // The function scope
  FunctionSemaScope function_scope(this, node);

  // Declare function parameters in scope
  for (const auto &param : func_type->GetParams()) {
    TPL_ASSERT(scope_ != nullptr, "No scope exists!");
    scope_->Declare(param.name, param.type);
  }

  // Recurse into the function body
  Visit(node->GetBody());

  // Check the return value. We allow functions to be empty or elide a final
  // "return" statement only if the function has a "nil" return type. In this
  // case, we automatically insert a "return" statement.
  if (node->IsEmpty() || !ast::Statement::IsTerminating(node->GetBody())) {
    if (!func_type->GetReturnType()->IsNilType()) {
      error_reporter_->Report(node->GetBody()->GetRightBracePosition(),
                              ErrorMessages::kMissingReturn);
      return;
    }

    auto *empty_ret = context_->GetNodeFactory()->NewReturnStatement(node->Position(), nullptr);
    node->GetBody()->AppendStatement(empty_ret);
  }
}

void Sema::VisitIdentifierExpression(ast::IdentifierExpression *node) {
  // Check the current context
  if (auto *type = scope_->Lookup(node->GetName())) {
    node->SetType(type);
    return;
  }

  // Check the builtin types
  if (auto *type = context_->LookupBuiltinType(node->GetName())) {
    node->SetType(type);
    return;
  }

  // Error
  error_reporter_->Report(node->Position(), ErrorMessages::kUndefinedVariable, node->GetName());
}

void Sema::VisitImplicitCastExpression(ast::ImplicitCastExpression *node) {
  throw std::runtime_error("Should never perform semantic checking on implicit cast expressions");
}

void Sema::VisitIndexExpression(ast::IndexExpression *node) {
  ast::Type *obj_type = Resolve(node->GetObject());
  ast::Type *index_type = Resolve(node->GetIndex());

  if (obj_type == nullptr || index_type == nullptr) {
    return;
  }

  if (!obj_type->IsArrayType() && !obj_type->IsMapType()) {
    error_reporter_->Report(node->Position(), ErrorMessages::kInvalidIndexOperation, obj_type);
    return;
  }

  if (!index_type->IsIntegerType()) {
    error_reporter_->Report(node->Position(), ErrorMessages::kNonIntegerArrayIndexValue);
    return;
  }

  if (auto arr_type = obj_type->SafeAs<ast::ArrayType>()) {
    if (auto index = node->GetIndex()->SafeAs<ast::LiteralExpression>()) {
      const int64_t index_val = index->IntegerVal();
      // Check negative array indices.
      if (index_val < 0) {
        error_reporter_->Report(index->Position(), ErrorMessages::kNegativeArrayIndexValue,
                                index_val);
        return;
      }
      // Check known out-of-bounds array access.
      if (arr_type->HasKnownLength() && static_cast<uint64_t>(index_val) >= arr_type->GetLength()) {
        error_reporter_->Report(index->Position(), ErrorMessages::kOutOfBoundsArrayIndexValue,
                                index_val, arr_type->GetLength());
        return;
      }
    }
    node->SetType(arr_type->GetElementType());
  } else {
    node->SetType(obj_type->As<ast::MapType>()->GetValueType());
  }
}

void Sema::VisitLiteralExpression(ast::LiteralExpression *node) {
  switch (node->GetLiteralKind()) {
    case ast::LiteralExpression::LiteralKind::Nil: {
      node->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
      break;
    }
    case ast::LiteralExpression::LiteralKind::Boolean: {
      node->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
      break;
    }
    case ast::LiteralExpression::LiteralKind::Float: {
      // Initially try to fit it as a 32-bit float, otherwise a 64-bit double.
      if (node->IsRepresentable(GetBuiltinType(ast::BuiltinType::Float32))) {
        node->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Float32));
      } else {
        node->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Float64));
      }
      break;
    }
    case ast::LiteralExpression::LiteralKind::Int: {
      // Initially try to fit the literal as a 32-bit signed integer. If the
      // value is not representable with 32 bits, use 64-bits. There isn't
      // another option because TPL does not currently support big integers.
      if (node->IsRepresentable(GetBuiltinType(ast::BuiltinType::Int32))) {
        node->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int32));
      } else {
        node->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int64));
      }
      break;
    }
    case ast::LiteralExpression::LiteralKind::String: {
      node->SetType(ast::StringType::Get(context_));
      break;
    }
  }
}

void Sema::VisitUnaryOpExpression(ast::UnaryOpExpression *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->GetInput());

  if (expr_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::BANG: {
      if (!expr_type->IsBoolType()) {
        error_reporter_->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                expr_type);
        return;
      }

      node->SetType(expr_type);
      break;
    }
    case parsing::Token::Type::MINUS: {
      if (!expr_type->IsArithmetic()) {
        error_reporter_->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                expr_type);
        return;
      }

      node->SetType(expr_type);
      break;
    }
    case parsing::Token::Type::STAR: {
      if (!expr_type->IsPointerType()) {
        error_reporter_->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                expr_type);
        return;
      }

      node->SetType(expr_type->As<ast::PointerType>()->GetBase());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      if (expr_type->IsFunctionType()) {
        error_reporter_->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                expr_type);
        return;
      }

      node->SetType(expr_type->PointerTo());
      break;
    }
    default: {
      UNREACHABLE("Impossible unary operation!");
    }
  }
}

void Sema::VisitMemberExpression(ast::MemberExpression *node) {
  ast::Type *obj_type = Resolve(node->GetObject());

  if (obj_type == nullptr) {
    // Some error
    return;
  }

  if (auto *pointer_type = obj_type->SafeAs<ast::PointerType>()) {
    obj_type = pointer_type->GetBase();
  }

  if (!obj_type->IsStructType()) {
    error_reporter_->Report(node->Position(), ErrorMessages::kMemberObjectNotComposite, obj_type);
    return;
  }

  if (!node->GetMember()->IsIdentifierExpression()) {
    error_reporter_->Report(node->GetMember()->Position(),
                            ErrorMessages::kExpectedIdentifierForMember);
    return;
  }

  ast::Identifier member = node->GetMember()->As<ast::IdentifierExpression>()->GetName();

  ast::Type *member_type = obj_type->As<ast::StructType>()->LookupFieldByName(member);

  if (member_type == nullptr) {
    error_reporter_->Report(node->GetMember()->Position(), ErrorMessages::kFieldObjectDoesNotExist,
                            member, obj_type);
    return;
  }

  node->SetType(member_type);
}

}  // namespace tpl::sema
