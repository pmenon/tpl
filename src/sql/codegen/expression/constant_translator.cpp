#include "sql/codegen/expression/constant_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/generic_value.h"

namespace tpl::sql::codegen {

ConstantTranslator::ConstantTranslator(const planner::ConstantValueExpression &expr,
                                       CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

edsl::ValueVT ConstantTranslator::DeriveValue(ConsumerContext *context,
                                              const ColumnValueProvider *provider) const {
  const auto &val = GetExpressionAs<const planner::ConstantValueExpression>().GetValue();
  switch (val.GetTypeId()) {
    case TypeId::Boolean:
      return edsl::BoolToSql(codegen_, val.value_.boolean);
    case TypeId::TinyInt:
      return edsl::IntToSql(codegen_, val.value_.tinyint);
    case TypeId::SmallInt:
      return edsl::IntToSql(codegen_, val.value_.smallint);
    case TypeId::Integer:
      return edsl::IntToSql(codegen_, val.value_.integer);
    case TypeId::BigInt:
      return edsl::IntToSql(codegen_, val.value_.bigint);
    case TypeId::Float:
      return edsl::FloatToSql(codegen_, val.value_.float_);
    case TypeId::Double:
      return edsl::FloatToSql(codegen_, val.value_.double_);
    case TypeId::Date: {
      int32_t year, month, day;
      val.value_.date_.ExtractComponents(&year, &month, &day);
      return edsl::DateToSql(codegen_, year, month, day);
    }
    case TypeId::Varchar:
      return edsl::StringToSql(codegen_, val.str_value_);
    default:
      throw NotImplementedException(
          fmt::format("Translation of constant type {}", TypeIdToString(val.GetTypeId())));
  }
}

}  // namespace tpl::sql::codegen
