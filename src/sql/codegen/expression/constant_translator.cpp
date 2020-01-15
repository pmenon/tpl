#include "sql/codegen/expression/constant_translator.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/consumer_context.h"
#include "sql/generic_value.h"

namespace tpl::sql::codegen {

ConstantTranslator::ConstantTranslator(const planner::ConstantValueExpression &expr,
                                       CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ConstantTranslator::DeriveValue(UNUSED ConsumerContext *ctx) const {
  CodeGen *codegen = GetCodeGen();

  const GenericValue &val = GetExpressionAs<const planner::ConstantValueExpression>().GetValue();
  switch (val.GetTypeId()) {
    case TypeId::Boolean:
      return codegen->BoolToSql(val.value_.boolean);
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
      return codegen->IntToSql(val.value_.bigint);
    case TypeId::Float:
    case TypeId::Double:
      return codegen->FloatToSql(val.value_.double_);
    case TypeId::Date:
      return codegen->DateToSql(val.value_.date_);
    case TypeId::Varchar:
      return codegen->StringToSql(val.str_value_);
    default:
      throw NotImplementedException("Translation of constant type {}",
                                    TypeIdToString(val.GetTypeId()));
  }
}

}  // namespace tpl::sql::codegen
