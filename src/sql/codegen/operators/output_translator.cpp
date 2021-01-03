#include "sql/codegen/operators/output_translator.h"

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "sql/codegen/compilation_context.h"
#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"
#include "sql/codegen/loop.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"

namespace tpl::sql::codegen {

namespace {
constexpr std::string_view kOutputColPrefix = "out";
}  // namespace

OutputTranslator::OutputTranslator(const planner::AbstractPlanNode &plan,
                                   CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      out_struct_(codegen_, "OutputStruct", false) {
  compilation_context->Prepare(plan, pipeline);
}

void OutputTranslator::Consume(ConsumerContext *context, FunctionBuilder *function) const {
  auto bytes = edsl::ResultBufferAllocOutRow(GetExecutionContext());
  function->Append(edsl::Declare(*out_row_, edsl::PtrCast(out_struct_.GetPtrToType(), bytes)));

  // for col in row:
  //  out.col = col
  const auto child_translator = GetCompilationContext()->LookupTranslator(GetPlan());
  for (uint32_t attr_idx = 0; attr_idx < GetPlan().GetOutputSchema()->NumColumns(); attr_idx++) {
    auto lhs = out_struct_.MemberGeneric(*out_row_, attr_idx);
    auto rhs = child_translator->GetOutput(context, attr_idx);
    function->Append(edsl::Assign(lhs, rhs));
  }
}

void OutputTranslator::FinishPipelineWork(UNUSED const PipelineContext &pipeline_ctx,
                                          FunctionBuilder *function) const {
  function->Append(edsl::ResultBufferFinalize(GetExecutionContext()));
}

void OutputTranslator::DefineStructsAndFunctions() {
  const auto output_schema = GetPlan().GetOutputSchema();
  for (uint32_t i = 0; i < output_schema->NumColumns(); i++) {
    auto name = fmt::format("{}{}", kOutputColPrefix, i);
    auto type = codegen_->GetTPLType(output_schema->GetColumn(i).GetExpr()->GetReturnValueType());
    out_struct_.AddMember(name, type);
  }
  out_struct_.Seal();

  // Finish up the variable.
  out_row_ = std::make_unique<edsl::VariableVT>(codegen_, "out_row", out_struct_.GetPtrToType());
}

}  // namespace tpl::sql::codegen
