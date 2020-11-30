#include "ast/ast.h"

#include <cfloat>
#include <cstdint>

#include "ast/type.h"

namespace tpl::ast {

// ---------------------------------------------------------
// Function Declaration
// ---------------------------------------------------------

FunctionDeclaration::FunctionDeclaration(const SourcePosition &pos, Identifier name,
                                         FunctionLiteralExpr *func)
    : Declaration(Kind::FunctionDeclaration, pos, name, func->GetTypeRepr()), func_(func) {}

// ---------------------------------------------------------
// Structure Declaration
// ---------------------------------------------------------

StructDeclaration::StructDeclaration(const SourcePosition &pos, Identifier name,
                                     StructTypeRepr *type_repr)
    : Declaration(Kind::StructDeclaration, pos, name, type_repr) {}

uint32_t StructDeclaration::NumFields() const {
  const auto &fields = GetTypeRepr()->As<ast::StructTypeRepr>()->GetFields();
  return fields.size();
}

ast::FieldDeclaration *StructDeclaration::GetFieldAt(uint32_t field_idx) const {
  return GetTypeRepr()->As<ast::StructTypeRepr>()->GetFieldAt(field_idx);
}

// ---------------------------------------------------------
// Expression Statement
// ---------------------------------------------------------

ExpressionStatement::ExpressionStatement(Expr *expr)
    : Statement(Kind::ExpressionStatement, expr->Position()), expr_(expr) {}

// ---------------------------------------------------------
// Expression
// ---------------------------------------------------------

bool Expr::IsNilLiteral() const {
  if (auto literal = SafeAs<ast::LiteralExpr>()) {
    return literal->IsNilLiteral();
  }
  return false;
}

bool Expr::IsBoolLiteral() const {
  if (auto literal = SafeAs<ast::LiteralExpr>()) {
    return literal->IsBoolLiteral();
  }
  return false;
}

bool Expr::IsStringLiteral() const {
  if (auto literal = SafeAs<ast::LiteralExpr>()) {
    return literal->IsStringLiteral();
  }
  return false;
}

bool Expr::IsIntegerLiteral() const {
  if (auto literal = SafeAs<ast::LiteralExpr>()) {
    return literal->IsIntegerLiteral();
  }
  return false;
}

// ---------------------------------------------------------
// Comparison Expression
// ---------------------------------------------------------

namespace {

// Catches: nil [ '==' | '!=' ] expr
bool MatchIsLiteralCompareNil(Expr *left, parsing::Token::Type op, Expr *right, Expr **result) {
  if (left->IsNilLiteral() && parsing::Token::IsCompareOp(op)) {
    *result = right;
    return true;
  }
  return false;
}

}  // namespace

bool ComparisonOpExpr::IsLiteralCompareNil(Expr **result) const {
  return MatchIsLiteralCompareNil(left_, op_, right_, result) ||
         MatchIsLiteralCompareNil(right_, op_, left_, result);
}

// ---------------------------------------------------------
// Function Literal Expressions
// ---------------------------------------------------------

FunctionLiteralExpr::FunctionLiteralExpr(FunctionTypeRepr *type_repr, BlockStatement *body)
    : Expr(Kind::FunctionLiteralExpr, type_repr->Position()), type_repr_(type_repr), body_(body) {}

// ---------------------------------------------------------
// Call Expression
// ---------------------------------------------------------

Identifier CallExpr::GetFuncName() const { return func_->As<IdentifierExpr>()->GetName(); }

// ---------------------------------------------------------
// Index Expressions
// ---------------------------------------------------------

bool IndexExpr::IsArrayAccess() const {
  TPL_ASSERT(GetObject() != nullptr, "Object cannot be NULL");
  TPL_ASSERT(GetObject() != nullptr, "Cannot determine object type before type checking!");
  return GetObject()->GetType()->IsArrayType();
}

bool IndexExpr::IsMapAccess() const {
  TPL_ASSERT(GetObject() != nullptr, "Object cannot be NULL");
  TPL_ASSERT(GetObject() != nullptr, "Cannot determine object type before type checking!");
  return GetObject()->GetType()->IsMapType();
}

// ---------------------------------------------------------
// Literal Expressions
// ---------------------------------------------------------

bool LiteralExpr::IsRepresentable(ast::Type *type) const {
  // Integers.
  if (type->IsIntegerType()) {
    if (!IsIntegerLiteral()) {
      return false;
    }
    const int64_t val = IntegerVal();
    // clang-format off
    switch (type->As<ast::BuiltinType>()->GetKind()) {
      case ast::BuiltinType::Kind::Int8:  return INT8_MIN <= val && val <= INT8_MAX;
      case ast::BuiltinType::Kind::Int16: return INT16_MIN <= val && val <= INT16_MAX;
      case ast::BuiltinType::Kind::Int32: return INT32_MIN <= val && val <= INT32_MAX;
      case ast::BuiltinType::Kind::Int64: return true;
      case ast::BuiltinType::Kind::UInt8: return 0 <= val && val <= int64_t(UINT8_MAX);
      case ast::BuiltinType::Kind::UInt16: return 0 <= val && val <= int64_t(UINT16_MAX);
      case ast::BuiltinType::Kind::UInt32: return 0 <= val && val <= int64_t(UINT32_MAX);
      case ast::BuiltinType::Kind::UInt64: return 0 <= val;
      default: UNREACHABLE("Impossible integer kind.");
    }
    // clang-format on
  }

  // Floats.
  if (type->IsFloatType()) {
    if (!IsFloatLiteral()) {
      return false;
    }
    const double val = FloatVal();
    switch (type->As<ast::BuiltinType>()->GetKind()) {
      case ast::BuiltinType::Kind::Float32: {
        const auto tmp = static_cast<float>(val);
        return FLT_MIN <= val && val <= FLT_MAX && static_cast<double>(tmp) == val;
      }
      case ast::BuiltinType::Kind::Float64: {
        return true;
      }
      default: {
        UNREACHABLE("Impossible integer kind.");
      }
    }
  }

  // Strings.
  if (type->IsStringType() && IsStringLiteral()) {
    return true;
  }

  // Booleans.
  if (type->IsBoolType() && IsBoolLiteral()) {
    return true;
  }

  // Nil.
  if (type->IsPointerType() && IsNilLiteral()) {
    return true;
  }

  return false;
}

// ---------------------------------------------------------
// Member expression
// ---------------------------------------------------------

bool MemberExpr::IsSugaredArrow() const {
  TPL_ASSERT(GetObject()->GetType() != nullptr,
             "Cannot determine sugared-arrow before type checking!");
  return GetObject()->GetType()->IsPointerType();
}

// ---------------------------------------------------------
// Statement
// ---------------------------------------------------------

bool Statement::IsTerminating(Statement *stmt) {
  switch (stmt->GetKind()) {
    case AstNode::Kind::BlockStatement: {
      return IsTerminating(stmt->As<BlockStatement>()->GetStatements().back());
    }
    case AstNode::Kind::IfStatement: {
      auto *if_stmt = stmt->As<IfStatement>();
      return (if_stmt->HasElseStatement() && (IsTerminating(if_stmt->GetThenStatement()) &&
                                              IsTerminating(if_stmt->GetElseStatement())));
    }
    case AstNode::Kind::ReturnStatement: {
      return true;
    }
    default: {
      return false;
    }
  }
}

std::string CastKindToString(const CastKind cast_kind) {
  switch (cast_kind) {
    case CastKind::IntToSqlInt:
      return "IntToSqlInt";
    case CastKind::IntToSqlDecimal:
      return "IntToSqlDecimal";
    case CastKind::SqlBoolToBool:
      return "SqlBoolToBool";
    case CastKind::BoolToSqlBool:
      return "BoolToSqlBool";
    case CastKind::IntegralCast:
      return "IntegralCast";
    case CastKind::BitCast:
      return "BitCast";
    case CastKind::FloatToSqlReal:
      return "FloatToSqlReal";
    case CastKind::SqlIntToSqlReal:
      return "SqlIntToSqlReal";
    default:
      UNREACHABLE("Impossible cast kind");
  }
}

}  // namespace tpl::ast
