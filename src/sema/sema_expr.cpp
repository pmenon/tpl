#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"
#include "logging/logger.h"

namespace tpl::sema {

void Sema::VisitBadExpr(ast::BadExpr *node) {
  TPL_ASSERT(false, "Bad expression in type checker!");
}

void Sema::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  ast::Type *left_type = Resolve(node->Left());
  ast::Type *right_type = Resolve(node->Right());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      auto [result_type, left, right] =
          CheckLogicalOperands(node->Op(), node->Position(), node->Left(), node->Right());
      node->SetType(result_type);
      if (node->Left() != left) node->SetLeft(left);
      if (node->Right() != right) node->SetRight(right);
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
          CheckArithmeticOperands(node->Op(), node->Position(), node->Left(), node->Right());
      node->SetType(result_type);
      if (node->Left() != left) node->SetLeft(left);
      if (node->Right() != right) node->SetRight(right);
      break;
    }
    default: {
      LOG_ERROR("{} is not a binary operation!", parsing::Token::GetString(node->Op()));
    }
  }
}

void Sema::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  ast::Type *left_type = Resolve(node->Left());
  ast::Type *right_type = Resolve(node->Right());

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
          CheckComparisonOperands(node->Op(), node->Position(), node->Left(), node->Right());
      node->SetType(result_type);
      if (node->Left() != left) node->SetLeft(left);
      if (node->Right() != right) node->SetRight(right);
      break;
    }
    default: {
      LOG_ERROR("{} is not a comparison operation", parsing::Token::GetString(node->Op()));
    }
  }
}

void Sema::VisitCallExpr(ast::CallExpr *node) {
  // If the call claims to be to a builtin, validate it
  if (node->GetCallKind() == ast::CallExpr::CallKind::Builtin) {
    CheckBuiltinCall(node);
    return;
  }

  // Resolve the function type
  ast::Type *type = Resolve(node->Function());
  if (type == nullptr) {
    return;
  }

  // Check that the resolved function type is actually a function
  auto *func_type = type->SafeAs<ast::FunctionType>();
  if (func_type == nullptr) {
    error_reporter()->Report(node->Position(), ErrorMessages::kNonFunction);
    return;
  }

  // Check argument count matches
  if (!CheckArgCount(node, func_type->GetNumParams())) {
    return;
  }

  // Resolve function arguments
  for (auto *arg : node->Arguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Check args
  bool has_errors = false;

  const auto &actual_args = node->Arguments();
  for (uint32_t arg_num = 0; arg_num < actual_args.size(); arg_num++) {
    ast::Type *expected_type = func_type->GetParams()[arg_num].type;
    ast::Expr *arg = actual_args[arg_num];

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

void Sema::VisitFunctionLiteralExpr(ast::FunctionLiteralExpr *node) {
  // Resolve the type, if not resolved already
  if (auto *type = node->TypeRepr()->GetType(); type == nullptr) {
    type = Resolve(node->TypeRepr());
    if (type == nullptr) {
      return;
    }
  }

  // Good function type, insert into node
  auto *func_type = node->TypeRepr()->GetType()->As<ast::FunctionType>();
  node->SetType(func_type);

  // The function scope
  FunctionSemaScope function_scope(this, node);

  // Declare function parameters in scope
  for (const auto &param : func_type->GetParams()) {
    current_scope()->Declare(param.name, param.type);
  }

  // Recurse into the function body
  Visit(node->Body());

  // Check the return value. We allow functions to be empty or elide a final
  // "return" statement only if the function has a "nil" return type. In this
  // case, we automatically insert a "return" statement.
  if (node->IsEmpty() || !ast::Stmt::IsTerminating(node->Body())) {
    if (!func_type->GetReturnType()->IsNilType()) {
      error_reporter()->Report(node->Body()->RightBracePosition(), ErrorMessages::kMissingReturn);
      return;
    }

    auto *empty_ret = context()->GetNodeFactory()->NewReturnStmt(node->Position(), nullptr);
    node->Body()->AppendStatement(empty_ret);
  }
}

void Sema::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Check the current context
  if (auto *type = current_scope()->Lookup(node->Name())) {
    node->SetType(type);
    return;
  }

  // Check the builtin types
  if (auto *type = context()->LookupBuiltinType(node->Name())) {
    node->SetType(type);
    return;
  }

  // Error
  error_reporter()->Report(node->Position(), ErrorMessages::kUndefinedVariable, node->Name());
}

void Sema::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  throw std::runtime_error("Should never perform semantic checking on implicit cast expressions");
}

void Sema::VisitIndexExpr(ast::IndexExpr *node) {
  ast::Type *obj_type = Resolve(node->Object());
  ast::Type *index_type = Resolve(node->Index());

  if (obj_type == nullptr || index_type == nullptr) {
    return;
  }

  if (!obj_type->IsArrayType() && !obj_type->IsMapType()) {
    error_reporter()->Report(node->Position(), ErrorMessages::kInvalidIndexOperation, obj_type);
    return;
  }

  if (!index_type->IsIntegerType()) {
    error_reporter()->Report(node->Position(), ErrorMessages::kNonIntegerArrayIndexValue);
    return;
  }

  if (auto arr_type = obj_type->SafeAs<ast::ArrayType>()) {
    if (auto index = node->Index()->SafeAs<ast::LiteralExpr>()) {
      const int64_t index_val = index->IntegerVal();
      // Check negative array indices.
      if (index_val < 0) {
        error_reporter()->Report(index->Position(), ErrorMessages::kNegativeArrayIndexValue,
                                 index_val);
        return;
      }
      // Check known out-of-bounds array access.
      if (arr_type->HasKnownLength() && static_cast<uint64_t>(index_val) >= arr_type->GetLength()) {
        error_reporter()->Report(index->Position(), ErrorMessages::kOutOfBoundsArrayIndexValue,
                                 index_val, arr_type->GetLength());
        return;
      }
    }
    node->SetType(arr_type->GetElementType());
  } else {
    node->SetType(obj_type->As<ast::MapType>()->GetValueType());
  }
}

void Sema::VisitLiteralExpr(ast::LiteralExpr *node) {
  switch (node->GetLiteralKind()) {
    case ast::LiteralExpr::LiteralKind::Nil: {
      node->SetType(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
      break;
    }
    case ast::LiteralExpr::LiteralKind::Boolean: {
      node->SetType(ast::BuiltinType::Get(context(), ast::BuiltinType::Bool));
      break;
    }
    case ast::LiteralExpr::LiteralKind::Float: {
      // Literal floats default to float32.
      node->SetType(ast::BuiltinType::Get(context(), ast::BuiltinType::Float32));
      break;
    }
    case ast::LiteralExpr::LiteralKind::Int: {
      // Literal integers default to int32.
      node->SetType(ast::BuiltinType::Get(context(), ast::BuiltinType::Int32));
      break;
    }
    case ast::LiteralExpr::LiteralKind::String: {
      node->SetType(ast::StringType::Get(context()));
      break;
    }
  }
}

void Sema::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->Input());

  if (expr_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::BANG: {
      if (!expr_type->IsBoolType()) {
        error_reporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                 expr_type);
        return;
      }

      node->SetType(expr_type);
      break;
    }
    case parsing::Token::Type::MINUS: {
      if (!expr_type->IsArithmetic()) {
        error_reporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                 expr_type);
        return;
      }

      node->SetType(expr_type);
      break;
    }
    case parsing::Token::Type::STAR: {
      if (!expr_type->IsPointerType()) {
        error_reporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
                                 expr_type);
        return;
      }

      node->SetType(expr_type->As<ast::PointerType>()->GetBase());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      if (expr_type->IsFunctionType()) {
        error_reporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(),
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

void Sema::VisitMemberExpr(ast::MemberExpr *node) {
  ast::Type *obj_type = Resolve(node->Object());

  if (obj_type == nullptr) {
    // Some error
    return;
  }

  if (auto *pointer_type = obj_type->SafeAs<ast::PointerType>()) {
    obj_type = pointer_type->GetBase();
  }

  if (!obj_type->IsStructType()) {
    error_reporter()->Report(node->Position(), ErrorMessages::kMemberObjectNotComposite, obj_type);
    return;
  }

  if (!node->Member()->IsIdentifierExpr()) {
    error_reporter()->Report(node->Member()->Position(),
                             ErrorMessages::kExpectedIdentifierForMember);
    return;
  }

  ast::Identifier member = node->Member()->As<ast::IdentifierExpr>()->Name();

  ast::Type *member_type = obj_type->As<ast::StructType>()->LookupFieldByName(member);

  if (member_type == nullptr) {
    error_reporter()->Report(node->Member()->Position(), ErrorMessages::kFieldObjectDoesNotExist,
                             member, obj_type);
    return;
  }

  node->SetType(member_type);
}

}  // namespace tpl::sema
