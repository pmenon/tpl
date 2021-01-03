#include "sql/codegen/operators/csv_scan_translator.h"

#include <string_view>

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compilation_context.h"
#include "sql/codegen/consumer_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/loop.h"
#include "sql/codegen/pipeline.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr std::string_view kFieldPrefix = "field";
}  // namespace

CSVScanTranslator::CSVScanTranslator(const planner::CSVScanPlanNode &plan,
                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      row_struct_(codegen_, "CSVRow", true) {
  // CSV scans are serial, for now.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
}

void CSVScanTranslator::DefineStructsAndFunctions() {
  // Construct the struct. It gets one sql::StringVal per column.
  for (uint32_t idx = 0; idx < GetPlan().GetOutputSchema()->NumColumns(); idx++) {
    auto name = fmt::format("{}{}", kFieldPrefix, idx);
    row_struct_.AddMember(name, codegen_->GetType<ast::x::StringVal>());
  }
  // Seal it all up.
  row_struct_.Seal();

  // Create the variable whose type is the CSV row type.
  row_var_ = std::make_unique<edsl::VariableVT>(codegen_, "csv_row", row_struct_.GetType());
}

edsl::Reference<ast::x::StringVal> CSVScanTranslator::GetField(uint32_t field_index) const {
  return row_struct_.Member<ast::x::StringVal>(*row_var_, field_index);
}

edsl::Value<ast::x::StringVal *> CSVScanTranslator::GetFieldPtr(uint32_t field_index) const {
  return row_struct_.MemberPtr<ast::x::StringVal>(*row_var_, field_index);
}

void CSVScanTranslator::DeclarePipelineState(PipelineContext *pipeline_ctx) {
  csv_reader_ =
      pipeline_ctx->DeclarePipelineStateEntry("csv_reader", codegen_->GetType<ast::x::CSVReader>());
  is_valid_reader_ =
      pipeline_ctx->DeclarePipelineStateEntry("is_valid_reader", codegen_->GetType<bool>());
}

void CSVScanTranslator::InitializePipelineState(const PipelineContext &pipeline_ctx,
                                                FunctionBuilder *function) const {
  auto csv_reader = pipeline_ctx.GetStateEntryPtr<ast::x::CSVReader>(csv_reader_);
  auto success = csv_reader->Init(GetCSVPlan().GetFileName());
  auto initialized_flag = pipeline_ctx.GetStateEntry<bool>(is_valid_reader_);
  function->Append(edsl::Assign(initialized_flag, success));
}

void CSVScanTranslator::TearDownPipelineState(const PipelineContext &pipeline_ctx,
                                              FunctionBuilder *function) const {
  auto csv_reader = pipeline_ctx.GetStateEntryPtr<ast::x::CSVReader>(csv_reader_);
  function->Append(csv_reader->Close());
}

void CSVScanTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  edsl::Variable<ast::x::CSVReader *> csv_reader(codegen_, "csv_reader");

  // var csv_row: CSVRow
  // var csv_reader = &q_state.csv_reader
  function->Append(edsl::Declare(*row_var_));
  function->Append(
      edsl::Declare(csv_reader, context->GetStateEntryPtr<ast::x::CSVReader>(csv_reader_)));

  // for (@csvReaderAdvance()) { ... }
  Loop scan_loop(function, csv_reader->Advance());
  {
    for (uint32_t i = 0; i < GetPlan().GetOutputSchema()->NumColumns(); i++) {
      function->Append(csv_reader->GetField(i, GetFieldPtr(i)));
    }
    context->Consume(function);
  }
  scan_loop.EndLoop();
}

edsl::ValueVT CSVScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto output_schema = GetPlan().GetOutputSchema();
  if (col_oid > output_schema->NumColumns()) {
    throw Exception(ExceptionType::CodeGen,
                    fmt::format("out-of-bounds CSV column access at idx={}", col_oid));
  }

  edsl::Value<ast::x::StringVal> field = GetField(col_oid);

  if (auto col_type = output_schema->GetColumn(col_oid).GetType(); col_type == TypeId::Varchar) {
    return field;
  } else {
    return edsl::ConvertSql(field, col_type);
  }

  return edsl::ConvertSql(field, GetPlan().GetOutputSchema()->GetColumn(col_oid).GetType());
}

void CSVScanTranslator::DrivePipeline(const PipelineContext &pipeline_ctx) const {
  TPL_ASSERT(pipeline_ctx.IsForPipeline(*GetPipeline()), "CSV scan driving unknown pipeline!");
  GetPipeline()->LaunchSerial(pipeline_ctx);
}

}  // namespace tpl::sql::codegen
