#include "sql/codegen/operators/csv_scan_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/pipeline.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr const char kFieldPrefix[] = "field";
}  // namespace

CSVScanTranslator::CSVScanTranslator(const planner::CSVScanPlanNode &plan,
                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      base_row_type_(codegen_->MakeFreshIdentifier("CSVRow")) {
  // CSV scans are serial, for now.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  // Declare state.
  base_row_ = compilation_context->GetQueryState()->DeclareStateEntry(
      codegen_, "csv_row", codegen_->MakeExpr(base_row_type_));
}

void CSVScanTranslator::DefineStructsAndFunctions() {
  // Reserve now to reduce allocations.
  const auto output_schema = GetPlan().GetOutputSchema();
  auto fields = codegen_->MakeEmptyFieldList();
  fields.reserve(output_schema->NumColumns());

  // Add columns to output.
  for (uint32_t idx = 0; idx < output_schema->NumColumns(); idx++) {
    auto field_name = codegen_->MakeIdentifier(kFieldPrefix + std::to_string(idx));
    fields.emplace_back(codegen_->MakeField(field_name, codegen_->TplType(TypeId::Varchar)));
  }
  codegen_->DeclareStruct(base_row_type_, std::move(fields));
}

ast::Expr *CSVScanTranslator::GetField(uint32_t field_index) const {
  ast::Identifier field_name = codegen_->MakeIdentifier(kFieldPrefix + std::to_string(field_index));
  return codegen_->AccessStructMember(GetQueryStateEntry(base_row_), field_name);
}

ast::Expr *CSVScanTranslator::GetFieldPtr(uint32_t field_index) const {
  return codegen_->AddressOf(GetField(field_index));
}

void CSVScanTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  auto reader_var_base = codegen_->MakeFreshIdentifier("csvReaderBase");
  auto reader_var = codegen_->MakeFreshIdentifier("csvReader");
  function->Append(codegen_->DeclareVarNoInit(reader_var_base, ast::BuiltinType::CSVReader));
  function->Append(codegen_->DeclareVarWithInit(reader_var, codegen_->AddressOf(reader_var_base)));
  function->Append(codegen_->DeclareVarWithInit(
      codegen_->MakeFreshIdentifier("isValid"),
      codegen_->CSVReaderInit(codegen_->MakeExpr(reader_var), GetCSVPlan().GetFileName())));
  Loop scan_loop(function, codegen_->CSVReaderAdvance(codegen_->MakeExpr(reader_var)));
  {
    // Read fields.
    const auto output_schema = GetPlan().GetOutputSchema();
    for (uint32_t i = 0; i < output_schema->NumColumns(); i++) {
      ast::Expr *field_ptr = GetFieldPtr(i);
      function->Append(codegen_->CSVReaderGetField(codegen_->MakeExpr(reader_var), i, field_ptr));
    }
    // Done.
    context->Consume(function);
  }
  scan_loop.EndLoop();
  function->Append(codegen_->CSVReaderClose(codegen_->MakeExpr(reader_var)));
}

ast::Expr *CSVScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto output_schema = GetPlan().GetOutputSchema();
  if (col_oid > output_schema->NumColumns()) {
    throw Exception(ExceptionType::CodeGen,
                    fmt::format("out-of-bounds CSV column access @ idx={}", col_oid));
  }

  // Return the field converted to the appropriate type.
  auto field = GetField(col_oid);
  auto output_type = GetPlan().GetOutputSchema()->GetColumn(col_oid).GetType();
  switch (output_type) {
    case TypeId::Boolean:
      return codegen_->CallBuiltin(ast::Builtin::ConvertStringToBool, {field});
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
      return codegen_->CallBuiltin(ast::Builtin::ConvertStringToInt, {field});
    case TypeId::Float:
    case TypeId::Double:
      return codegen_->CallBuiltin(ast::Builtin::ConvertStringToReal, {field});
    case TypeId::Date:
      return codegen_->CallBuiltin(ast::Builtin::ConvertStringToDate, {field});
    case TypeId::Timestamp:
      return codegen_->CallBuiltin(ast::Builtin::ConvertStringToTime, {field});
    case TypeId::Varchar:
      return field;
    default:
      throw NotImplementedException(
          fmt::format("converting from string to {}", TypeIdToString(output_type)));
  }
}

}  // namespace tpl::sql::codegen
