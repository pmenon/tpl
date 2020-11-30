#include "sql/codegen/expression/constant_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/consumer_context.h"
#include "sql/generic_value.h"

namespace tpl::sql::codegen {

ConstantTranslator::ConstantTranslator(const planner::ConstantValueExpression &expr,
                                       CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expression *ConstantTranslator::DeriveValue(ConsumerContext *context,
                                                 const ColumnValueProvider *provider) const {
  const auto &val = GetExpressionAs<const planner::ConstantValueExpression>().GetValue();
  switch (val.GetTypeId()) {
    case TypeId::Boolean:
      return codegen_->BoolToSql(val.value_.boolean);
    case TypeId::TinyInt:
      return codegen_->IntToSql(val.value_.tinyint);
    case TypeId::SmallInt:
      return codegen_->IntToSql(val.value_.smallint);
    case TypeId::Integer:
      return codegen_->IntToSql(val.value_.integer);
    case TypeId::BigInt:
      return codegen_->IntToSql(val.value_.bigint);
    case TypeId::Float:
      return codegen_->FloatToSql(val.value_.float_);
    case TypeId::Double:
      return codegen_->FloatToSql(val.value_.double_);
    case TypeId::Date:
      return codegen_->DateToSql(val.value_.date_);
    case TypeId::Varchar:
      return codegen_->StringToSql(val.str_value_);
    default:
      throw NotImplementedException(
          fmt::format("Translation of constant type {}", TypeIdToString(val.GetTypeId())));
  }
}

}  // namespace tpl::sql::codegen
