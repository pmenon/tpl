#include "sql/codegen/codegen.h"

#include "spdlog/fmt/fmt.h"

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "ast/type_visitor.h"
#include "common/exception.h"

namespace tpl::sql::codegen {

CodeGen::CodeGen(CompilationUnit *container)
    : container_(container), position_{0, 0}, function_(nullptr) {}

ast::Type *CodeGen::GetTPLType(SqlTypeId type_id) {
  switch (type_id) {
    case sql::SqlTypeId::Boolean:
      return GetType<ast::x::BooleanVal>();
    case sql::SqlTypeId::TinyInt:
    case sql::SqlTypeId::SmallInt:
    case sql::SqlTypeId::Integer:
    case sql::SqlTypeId::BigInt:
      return GetType<ast::x::IntegerVal>();
    case sql::SqlTypeId::Date:
      return GetType<ast::x::DateVal>();
    case sql::SqlTypeId::Timestamp:
      return GetType<ast::x::TimestampVal>();
    case sql::SqlTypeId::Real:
    case sql::SqlTypeId::Double:
      return GetType<ast::x::RealVal>();
    case sql::SqlTypeId::Varchar:
      return GetType<ast::x::StringVal>();
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Type *CodeGen::GetPrimitiveTPLType(TypeId type_id) {
  switch (type_id) {
    case sql::TypeId::Boolean:
      return GetType<bool>();
    case sql::TypeId::TinyInt:
      return GetType<int8_t>();
    case sql::TypeId::SmallInt:
      return GetType<int16_t>();
    case sql::TypeId::Integer:
      return GetType<int32_t>();
    case sql::TypeId::BigInt:
      return GetType<int64_t>();
    case sql::TypeId::Float:
      return GetType<float>();
    case sql::TypeId::Double:
      return GetType<double>();
    case sql::TypeId::Date:
      return GetType<ast::x::Date>();
    case sql::TypeId::Timestamp:
      return GetType<ast::x::Timestamp>();
    case sql::TypeId::Varchar:
      return GetType<ast::x::VarlenEntry>();
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Type *CodeGen::AggregateType(planner::AggregateKind agg_kind, TypeId ret_type) const {
  switch (agg_kind) {
    case planner::AggregateKind::COUNT_STAR:
      return GetType<ast::x::CountStarAggregate>();
    case planner::AggregateKind::COUNT:
      return GetType<ast::x::CountAggregate>();
    case planner::AggregateKind::AVG:
      return GetType<ast::x::AvgAggregate>();
    case planner::AggregateKind::MIN:
      if (IsTypeIntegral(ret_type)) {
        return GetType<ast::x::IntegerMinAggregate>();
      } else if (IsTypeFloatingPoint(ret_type)) {
        return GetType<ast::x::RealMinAggregate>();
      } else if (ret_type == TypeId::Date) {
        return GetType<ast::x::DateMinAggregate>();
      } else if (ret_type == TypeId::Varchar) {
        return GetType<ast::x::StringMinAggregate>();
      } else {
        throw NotImplementedException(
            fmt::format("MIN() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case planner::AggregateKind::MAX:
      if (IsTypeIntegral(ret_type)) {
        return GetType<ast::x::IntegerMaxAggregate>();
      } else if (IsTypeFloatingPoint(ret_type)) {
        return GetType<ast::x::RealMaxAggregate>();
      } else if (ret_type == TypeId::Date) {
        return GetType<ast::x::DateMaxAggregate>();
      } else if (ret_type == TypeId::Varchar) {
        return GetType<ast::x::StringMaxAggregate>();
      } else {
        throw NotImplementedException(
            fmt::format("MAX() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case planner::AggregateKind::SUM:
      TPL_ASSERT(IsTypeNumeric(ret_type), "Only arithmetic types have sums.");
      if (IsTypeIntegral(ret_type)) {
        return GetType<ast::x::IntegerSumAggregate>();
      }
      return GetType<ast::x::RealSumAggregate>();
    default: {
      UNREACHABLE("AggregateType() should only be called with aggregates.");
    }
  }
}

ast::ArrayType *CodeGen::ArrayType(uint32_t size, ast::Type *elem_type) {
  return ast::ArrayType::Get(size, elem_type);
}

ast::Expression *CodeGen::ConstBool(bool v) {
  auto result = NodeFactory()->NewBoolLiteral(position_, v);
  result->SetType(BoolType());
  return result;
}

ast::Expression *CodeGen::ConstInt(int64_t v) { return NodeFactory()->NewIntLiteral(position_, v); }

ast::Expression *CodeGen::ConstFloat(double v) {
  return NodeFactory()->NewFloatLiteral(position_, v);
}

ast::Expression *CodeGen::ConstString(std::string_view v) {
  return NodeFactory()->NewStringLiteral(position_, MakeIdentifier(v));
}

ast::Expression *CodeGen::Nil() const {
  auto nil_val = NodeFactory()->NewNilLiteral(position_);
  nil_val->SetType(NilType());
  return nil_val;
}

ast::Statement *CodeGen::DeclareVar(ast::Identifier name, ast::Type *type) {
  TPL_ASSERT(type_repr->GetType() != nullptr, "Type representation isn't typed!");
  auto type_repr = BuildTypeRepresentation(type, false);
  auto decl = NodeFactory()->NewVariableDeclaration(position_, name, type_repr, nullptr);
  return NodeFactory()->NewDeclStatement(decl);
}

ast::Statement *CodeGen::DeclareVarWithInit(ast::Identifier name, ast::Expression *val) {
  TPL_ASSERT(val->GetType() != nullptr, "Initial value to variable declaration isn't typed!");
  auto type_repr = BuildTypeRepresentation(val->GetType(), false);
  auto decl = NodeFactory()->NewVariableDeclaration(position_, name, type_repr, val);
  return NodeFactory()->NewDeclStatement(decl);
}

ast::StructType *CodeGen::DeclareStruct(ast::Identifier name,
                                        const std::vector<ast::Field> &members) const {
  auto type = ast::StructType::Get(Context(), name,
                                   {members.begin(), members.end(), Context()->GetRegion()});
  auto type_repr = BuildTypeRepresentation(type, true)->SafeAs<ast::StructTypeRepr>();
  TPL_ASSERT(type_repr != nullptr, "Representation of struct type isn't a struct representation?");
  auto decl = NodeFactory()->NewStructDeclaration(position_, name, type_repr);
  container_->RegisterStruct(decl);
  return type;
}

ast::Expression *CodeGen::AddressOf(ast::Expression *obj) const {
  TPL_ASSERT(obj->GetType() != nullptr, "Object we're taking address-of doesn't have type!");
  auto result =
      NodeFactory()->NewUnaryOpExpression(position_, parsing::Token::Type::AMPERSAND, obj);
  result->SetType(obj->GetType()->PointerTo());
  return result;
}

ast::Expression *CodeGen::Deref(ast::Expression *ptr) const {
  TPL_ASSERT(ptr->GetType() != nullptr, "Pointer expression being dereferenced doesn't have type!");
  TPL_ASSERT(ptr->GetType()->IsPointerType(), "Object being dereferenced isn't a pointer!");
  auto result = NodeFactory()->NewUnaryOpExpression(position_, parsing::Token::Type::STAR, ptr);
  result->SetType(ptr->GetType()->GetPointeeType());
  return result;
}

ast::Statement *CodeGen::Assign(ast::Expression *dest, ast::Expression *value) {
  TPL_ASSERT(dest->GetType() == value->GetType(), "Mismatched types!");
  return NodeFactory()->NewAssignmentStatement(position_, dest, value);
}

ast::Expression *CodeGen::ArrayAccess(ast::Expression *arr, ast::Expression *idx) {
  TPL_ASSERT(arr->GetType() != nullptr, "Input expression doesn't have type!");
  TPL_ASSERT(arr->GetType()->IsArrayType(), "Input to array access must be array type!");
  auto result = NodeFactory()->NewIndexExpression(position_, arr, idx);
  result->SetType(arr->GetType()->As<ast::ArrayType>()->GetElementType());
  return result;
}

ast::Expression *CodeGen::ArrayAccess(ast::Expression *arr, uint64_t idx) {
  return ArrayAccess(arr, Literal<uint64_t>(idx));
}

ast::Expression *CodeGen::IntCast(ast::Type *type, ast::Expression *input) const {
  TPL_ASSERT(type->IsIntegerType(), "Target type of @intCast() must be integral!");
  TPL_ASSERT(input->GetType() != nullptr, "Input argument to @intCast must have resolved type!");
  TPL_ASSERT(input->GetType()->IsIntegerType(), "Input argument is not an integral type!");
  auto type_repr = BuildTypeRepresentation(type, false);
  auto result = CallBuiltin(ast::Builtin::IntCast, {type_repr, input});
  result->SetType(type);
  return result;
}

ast::Expression *CodeGen::PtrCast(ast::Type *type, ast::Expression *input) const {
  TPL_ASSERT(type->IsPointerType(), "Target of @ptrCast() must be pointer type!");
  TPL_ASSERT(input->GetType() != nullptr, "Input argument to @ptrCast must have resolved type!");
  TPL_ASSERT(input->GetType()->IsPointerType(), "Input argument is not pointer type!");
  auto type_repr = BuildTypeRepresentation(type, false);
  auto result = CallBuiltin(ast::Builtin::PtrCast, {type_repr, input});
  result->SetType(type);
  return result;
}

ast::Expression *CodeGen::StructMember(ast::Expression *obj, std::string_view member) {
  TPL_ASSERT(obj->GetType() != nullptr, "Input expression doesn't have a type!");
  TPL_ASSERT(obj->GetType()->Is<ast::StructType>() ||
                 obj->GetType()->GetPointeeType()->Is<ast::StructType>(),
             "Input type isn't a struct or pointer-to-struct!");
  auto member_name = MakeIdentifier(member);
  auto result = NodeFactory()->NewMemberExpression(position_, obj, MakeExpr(member_name));
  auto struct_type = obj->GetType()->SafeAs<ast::StructType>();
  if (struct_type == nullptr) {  // Pointer to struct.
    struct_type = obj->GetType()->GetPointeeType()->As<ast::StructType>();
  }
  result->SetType(struct_type->LookupFieldByName(member_name));
  return result;
}

ast::Statement *CodeGen::Return() { return Return(nullptr); }

ast::Statement *CodeGen::Return(ast::Expression *ret) {
  auto stmt = NodeFactory()->NewReturnStatement(position_, ret);
  NewLine();
  return stmt;
}

ast::Expression *CodeGen::BooleanOp(parsing::Token::Type op, ast::Expression *lhs,
                                    ast::Expression *rhs) {
  TPL_ASSERT(lhs->GetType() != nullptr, "Left input doesn't have type!");
  TPL_ASSERT(rhs->GetType() != nullptr, "Right input doesn't have type!");
  TPL_ASSERT(lhs->GetType()->IsBoolType() && lhs->GetType() == right->GetType(),
             "Both inputs to logical-AND must be boolean!");

  auto result = NodeFactory()->NewBinaryOpExpression(position_, op, lhs, rhs);
  result->SetType(lhs->GetType());
  return result;
}

ast::Expression *CodeGen::ArithmeticOp(parsing::Token::Type op, ast::Expression *lhs,
                                       ast::Expression *rhs) {
  TPL_ASSERT(lhs->GetType() != nullptr && lhs->GetType() == rhs->GetType(),
             "Missing or mismatched types in binary math operation.");
  TPL_ASSERT(lhs->GetType()->IsArithmetic(), "Binary operation on non-arithmetic inputs!");
  TPL_ASSERT(op != parsing::Token::Type::PERCENT || lhs->GetType()->IsFloatType(),
             "Floating point operation not allowed.");
  auto result = NodeFactory()->NewBinaryOpExpression(position_, op, lhs, rhs);
  result->SetType(lhs->GetType());
  return result;
}

ast::Expression *CodeGen::ComparisonOp(parsing::Token::Type op, ast::Expression *lhs,
                                       ast::Expression *rhs) {
  TPL_ASSERT(lhs->GetType() != nullptr && lhs->GetType() == rhs->GetType(),
             "Missing or mismatched types in comparison operation.");
  TPL_ASSERT(parsing::Token::IsCompareOp(op), "Provided operation isn't a comparison operation!");
  auto result = NodeFactory()->NewComparisonOpExpression(position_, op, lhs, rhs);
  result->SetType(GetType<bool>());
  return result;
}

ast::Expression *CodeGen::Neg(ast::Expression *input) {
  TPL_ASSERT(input->GetType() != nullptr, "Input to negation doesn't have a type.");
  TPL_ASSERT(input->GetType()->IsArithmetic(), "Input to negation isn't an arithmetic type.");
  auto result = NodeFactory()->NewUnaryOpExpression(position_, parsing::Token::Type::MINUS, input);
  result->SetType(input->GetType());
  return result;
}

ast::Expression *CodeGen::Add(ast::Expression *lhs, ast::Expression *rhs) {
  return ArithmeticOp(parsing::Token::Type::PLUS, lhs, rhs);
}

ast::Expression *CodeGen::Sub(ast::Expression *lhs, ast::Expression *rhs) {
  return ArithmeticOp(parsing::Token::Type::MINUS, lhs, rhs);
}

ast::Expression *CodeGen::Mul(ast::Expression *lhs, ast::Expression *rhs) {
  return ArithmeticOp(parsing::Token::Type::STAR, lhs, rhs);
}

ast::Expression *CodeGen::Div(ast::Expression *lhs, ast::Expression *rhs) {
  return ArithmeticOp(parsing::Token::Type::SLASH, lhs, rhs);
}

ast::Expression *CodeGen::Mod(ast::Expression *lhs, ast::Expression *rhs) {
  return ArithmeticOp(parsing::Token::Type::PERCENT, lhs, rhs);
}

ast::Expression *CodeGen::CompareEq(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::EQUAL_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::CompareNe(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::BANG_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::CompareLt(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::LESS, lhs, rhs);
}

ast::Expression *CodeGen::CompareLe(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::LESS_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::CompareGt(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::GREATER, lhs, rhs);
}

ast::Expression *CodeGen::CompareGe(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::GREATER_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::LogicalAnd(ast::Expression *lhs, ast::Expression *rhs) {
  return BooleanOp(parsing::Token::Type::AND, lhs, rhs);
}

ast::Expression *CodeGen::LogicalOr(ast::Expression *lhs, ast::Expression *rhs) {
  return BooleanOp(parsing::Token::Type::OR, lhs, rhs);
}

ast::Expression *CodeGen::LogicalNot(ast::Expression *input) {
  TPL_ASSERT(input->GetType() != nullptr || input->GetType()->IsBoolType(),
             "Input to logical-not must be primitive boolean.");
  ast::Expression *result =
      NodeFactory()->NewUnaryOpExpression(position_, parsing::Token::Type::BANG, input);
  result->SetType(input->GetType());
  return result;
}

ast::Expression *CodeGen::Call(ast::Identifier func_name,
                               std::initializer_list<ast::Expression *> args) const {
  util::RegionVector<ast::Expression *> call_args(args, Context()->GetRegion());
  return NodeFactory()->NewCallExpression(MakeExpr(func_name), std::move(call_args));
}

ast::Expression *CodeGen::Call(ast::Identifier func_name,
                               const std::vector<ast::Expression *> &args) const {
  util::RegionVector<ast::Expression *> call_args(args.begin(), args.end(), Context()->GetRegion());
  return NodeFactory()->NewCallExpression(MakeExpr(func_name), std::move(call_args));
}

ast::Expression *CodeGen::CallBuiltin(ast::Builtin builtin,
                                      std::initializer_list<ast::Expression *> args) const {
  // We expect caller to set the return type.
  util::RegionVector<ast::Expression *> call_args(args, Context()->GetRegion());
  auto func_name = MakeExpr(Context()->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  return NodeFactory()->NewBuiltinCallExpression(func_name, std::move(call_args));
}

// This is copied from the overloaded function. But, we use initializer so often we keep it around.
ast::Expression *CodeGen::CallBuiltin(ast::Builtin builtin,
                                      const std::vector<ast::Expression *> &args) const {
  // We expect caller to set the return type.
  util::RegionVector<ast::Expression *> call_args(args.begin(), args.end(), Context()->GetRegion());
  auto func = MakeExpr(Context()->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  return NodeFactory()->NewBuiltinCallExpression(func, std::move(call_args));
}

ast::Identifier CodeGen::MakeFreshIdentifier(std::string_view name) {
  // Attempt insert.
  auto insert_result = names_.insert(std::make_pair(name, 1));
  if (insert_result.second) {
    return Context()->GetIdentifier(insert_result.first->getKey());
  }

  // Duplicate found. Find a new version that hasn't already been declared.
  uint64_t &id = insert_result.first->getValue();
  while (true) {
    const std::string next_name = fmt::format("{}{}", name, id++);
    if (names_.find(next_name) == names_.end()) {
      return Context()->GetIdentifier(next_name);
    }
  }
}

ast::Identifier CodeGen::MakeIdentifier(std::string_view str) const {
  return Context()->GetIdentifier(str);
}

ast::Expression *CodeGen::MakeExpr(ast::Identifier ident) const {
  return NodeFactory()->NewIdentifierExpression(position_, ident);
}

ast::Statement *CodeGen::MakeStatement(ast::Expression *expression) const {
  return NodeFactory()->NewExpressionStatement(expression);
}

ast::BlockStatement *CodeGen::MakeEmptyBlock() const {
  return NodeFactory()->NewBlockStatement(position_, position_, {{}, Context()->GetRegion()});
}

ast::AstNodeFactory *CodeGen::NodeFactory() const { return Context()->GetNodeFactory(); }

namespace {

class TypeReprBuilder : public ast::TypeVisitor<TypeReprBuilder, ast::Expression *> {
 public:
  TypeReprBuilder(SourcePosition pos, ast::Context *context, const ast::Type *root, bool for_struct)
      : pos_(pos), context_(context), root_(root), for_struct_(for_struct) {}

  ast::Expression *GetRepresentation() { return Visit(root_); }

#define DECLARE_VISIT_TYPE(Type) ast::Expression *Visit##Type(const ast::Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

 private:
  ast::AstNodeFactory *GetFactory() { return context_->GetNodeFactory(); }

 private:
  SourcePosition pos_;
  ast::Context *context_;
  const ast::Type *root_;
  bool for_struct_;
};

ast::Expression *TypeReprBuilder::VisitPointerType(const ast::PointerType *type) {
  return GetFactory()->NewPointerType(pos_, Visit(type->GetBase()));
}

ast::Expression *TypeReprBuilder::VisitBuiltinType(const ast::BuiltinType *type) {
  return GetFactory()->NewIdentifierExpression(pos_, context_->GetIdentifier(type->GetTplName()));
}

ast::Expression *TypeReprBuilder::VisitArrayType(const ast::ArrayType *type) {
  ast::Expression *length = nullptr, *element_type = Visit(type->GetElementType());
  if (type->HasKnownLength()) {
    length = GetFactory()->NewIntLiteral(pos_, type->GetLength());
  }
  return GetFactory()->NewArrayType(pos_, length, element_type);
}

ast::Expression *TypeReprBuilder::VisitMapType(const ast::MapType *type) {
  ast::Expression *key_type = Visit(type->GetKeyType());
  ast::Expression *val_type = Visit(type->GetValueType());
  return GetFactory()->NewMapType(pos_, key_type, val_type);
}

ast::Expression *TypeReprBuilder::VisitStructType(const ast::StructType *type) {
  if (type->IsNamed()) {
    if (!for_struct_ || type != root_) {
      return GetFactory()->NewIdentifierExpression(pos_, type->GetName());
    }
  }

  util::RegionVector<ast::FieldDeclaration *> fields(context_->GetRegion());
  fields.reserve(type->GetFields().size());
  for (const auto &[name, type] : type->GetFields()) {
    fields.push_back(GetFactory()->NewFieldDeclaration(pos_, name, Visit(type)));
  }
  return GetFactory()->NewStructType(pos_, std::move(fields));
}

ast::Expression *TypeReprBuilder::VisitStringType(const ast::StringType *type) {
  // TODO(pmenon): Fill me in.
  return TypeVisitor::VisitStringType(type);
}

ast::Expression *TypeReprBuilder::VisitFunctionType(const ast::FunctionType *type) {
  // The parameters.
  util::RegionVector<ast::FieldDeclaration *> params(context_->GetRegion());
  params.reserve(type->GetNumParams());
  for (const auto &field : type->GetParams()) {
    params.emplace_back(GetFactory()->NewFieldDeclaration(pos_, field.name, Visit(field.type)));
  }
  // The return type.
  ast::Expression *ret_type = Visit(type->GetReturnType());
  // Done.
  return GetFactory()->NewFunctionType(pos_, std::move(params), ret_type);
}

}  // namespace

ast::Expression *CodeGen::BuildTypeRepresentation(ast::Type *type, bool for_struct) const {
  TypeReprBuilder builder(position_, Context(), type, for_struct);
  ast::Expression *type_repr = builder.GetRepresentation();
  type_repr->SetType(type);
  return type_repr;
}

ast::Expression *CodeGen::MakeExpr(ast::Identifier name, ast::Type *type) {
  TPL_ASSERT(type != nullptr, "Input type to assign expression cannot be NULL.");
  ast::Expression *result = MakeExpr(name);
  result->SetType(type);
  return result;
}

}  // namespace tpl::sql::codegen
