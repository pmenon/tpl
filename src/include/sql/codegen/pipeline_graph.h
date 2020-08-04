#pragma once

#include <unordered_map>
#include <unordered_set>

#include "llvm/ADT/SmallVector.h"

#include "common/macros.h"
#include "sql/codegen/codegen_defs.h"

namespace tpl::sql::codegen {

class Pipeline;

/**
 * A pipeline dependency graph. Used to capture an execution order between pipelines making up a
 * query plan.
 */
class PipelineGraph {
 public:
  /**
   * Default construct a pipeline.
   */
  PipelineGraph() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(PipelineGraph);

  /**
   * @return The next available pipeline ID.
   */
  PipelineId NextPipelineId();

  /**
   * @return True if provided pipeline is registered in this graph.
   */
  bool IsRegistered(const Pipeline &pipeline) const;

  /**
   * Register the provided pipeline in this graph. It initially has no dependencies.
   * @param pipeline The pipeline.
   */
  void RegisterPipeline(const Pipeline &pipeline);

  /**
   * Create and add the dependency relation, @em a depends on @em b, to this graph.
   * @param a The dependent pipeline.
   * @param b The dependency pipeline.
   */
  void AddDependency(const Pipeline &a, const Pipeline &b);

  /**
   * Collect the set of pipelines that are required to execute
   * @param pipeline
   */
  void CollectTransitiveDependencies(const Pipeline &pipeline,
                                     std::vector<const Pipeline *> *dependencies) const;

 private:
  // The next available pipeline ID.
  PipelineId next_id_{0};

  // Type-def used to represent the set of dependents. We expect pipelines to
  // typically require only one dependency, so we use a small vector.
  // However, there scenarios where pipelines have more than one dependency.
  using DependencySet = llvm::SmallVector<const Pipeline *, 2>;

  // Each pipeline has a set of pipeline's it depends on. We use this structure
  // as a type of adjacency list.
  std::unordered_map<const Pipeline *, DependencySet> dependency_graph_;
};

}  // namespace tpl::sql::codegen
