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
      base_row_type_(codegen_->MakeFreshIdentifier("CSVRow")),
      row_var_(codegen_->MakeFreshIdentifier("csv_row")) {
  // CSV scans are serial, for now.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
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

ast::Expression *CSVScanTranslator::GetField(ast::Identifier row, uint32_t field_index) const {
  ast::Identifier field_name = codegen_->MakeIdentifier(kFieldPrefix + std::to_string(field_index));
  return codegen_->AccessStructMember(codegen_->MakeExpr(row), field_name);
}

ast::Expression *CSVScanTranslator::GetFieldPtr(ast::Identifier row, uint32_t field_index) const {
  return codegen_->AddressOf(GetField(row, field_index));
}

void CSVScanTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  csv_reader_ = pipeline_ctx->DeclarePipelineStateEntry(
      "csv_reader", codegen_->BuiltinType(ast::BuiltinType::CSVReader));
  is_valid_reader_ = pipeline_ctx->DeclarePipelineStateEntry(
      "is_valid_reader", codegen_->BuiltinType(ast::BuiltinType::Bool));
}

void CSVScanTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                FunctionBuilder *function) const {
  // @csvReaderInit()
  ast::Expression *csv_reader = pipeline_ctx.GetStateEntryPtr(csv_reader_);
  const std::string &csv_filename = GetCSVPlan().GetFileName();
  ast::Expression *init_call = codegen_->CSVReaderInit(csv_reader, csv_filename);
  // Assign boolean validation flag.
  function->Append(codegen_->Assign(pipeline_ctx.GetStateEntry(is_valid_reader_), init_call));
}

void CSVScanTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                              FunctionBuilder *function) const {
  ast::Expression *csv_reader = pipeline_ctx.GetStateEntryPtr(csv_reader_);
  function->Append(codegen_->CSVReaderClose(csv_reader));
}

void CSVScanTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  // var csv_row: CSVRow
  function->Append(codegen_->DeclareVarNoInit(row_var_, codegen_->MakeExpr(base_row_type_)));

  // for (@csvReaderAdvance()) { ... }
  Loop scan_loop(function, codegen_->CSVReaderAdvance(context->GetStateEntryPtr(csv_reader_)));
  {
    const auto num_output_cols = GetPlan().GetOutputSchema()->NumColumns();
    for (uint32_t idx = 0; idx < num_output_cols; idx++) {
      ast::Expression *csv_reader = context->GetStateEntryPtr(csv_reader_);
      function->Append(codegen_->CSVReaderGetField(csv_reader, idx, GetFieldPtr(row_var_, idx)));
    }
    context->Consume(function);
  }
  scan_loop.EndLoop();
}

ast::Expression *CSVScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto output_schema = GetPlan().GetOutputSchema();
  if (col_oid > output_schema->NumColumns()) {
    throw Exception(ExceptionType::CodeGen,
                    fmt::format("out-of-bounds CSV column access @ idx={}", col_oid));
  }

  // Return the field converted to the appropriate type.
  ast::Expression *field = GetField(row_var_, col_oid);
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

void CSVScanTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "CSV scan driving unknown pipeline!");
  GetPipeline()->LaunchSerial(pipeline_ctx);
}

}  // namespace tpl::sql::codegen
