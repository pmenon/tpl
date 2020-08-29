#pragma once

#include <vector>

#include "ast/identifier.h"
#include "sql/codegen/ast_fwd.h"

namespace tpl::sql::codegen {

class FunctionBuilder;
class Pipeline;

/**
 * Interface for any operator that drives a pipeline. Pipeline drivers are responsible for launching
 * parallel work.
 */
class PipelineDriver {
 public:
  /**
   * No-op virtual destructor.
   */
  virtual ~PipelineDriver() = default;

  /**
   * Drive the pipeline.
   * @param pipeline_ctx The pipeline context.
   */
  virtual void DrivePipeline(const PipelineContext &pipeline_ctx) const = 0;
};

}  // namespace tpl::sql::codegen
