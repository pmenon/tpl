#pragma once

#include "ast/identifier.h"
#include "sql/codegen/ast_fwd.h"
#include "util/region_containers.h"

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
   * @return The list of extra fields added to the "work" function. By default, the first two
   *         arguments are the query state and the pipeline state.
   */
  virtual util::RegionVector<ast::FieldDecl *> GetWorkerParams() const = 0;

  /**
   * This is called to launch the provided worker function in parallel across a set of threads.
   * @param work_func_name The name of the work function that implements the pipeline logic.
   */
  virtual void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const = 0;
};

}  // namespace tpl::sql::codegen
