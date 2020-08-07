#pragma once

namespace tpl::sql::codegen {

/**
 * IDs used to distinguish pipelines within a query.
 */
using PipelineId = uint32_t;

/**
 * An enumeration capturing the mode of code generation when compiling SQL queries to TPL.
 */
enum class CompilationMode : uint8_t {
  // One-shot compilation is when all TPL code for the whole query plan is
  // generated up-front at once before execution.
  OneShot,

  // Interleaved compilation is a mode that mixes code generation and execution.
  // Query fragments are generated and executed in lock step.
  Interleaved,
};

}  // namespace tpl::sql::codegen
