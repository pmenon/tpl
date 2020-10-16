#include "sql/codegen/pipeline_graph.h"

#include <algorithm>

#include "common/macros.h"
#include "sql/codegen/pipeline.h"

namespace tpl::sql::codegen {

uint32_t PipelineGraph::NextPipelineId() { return next_id_++; }

bool PipelineGraph::IsRegistered(const Pipeline &pipeline) const {
  const auto iter = dependency_graph_.find(&pipeline);
  return iter != dependency_graph_.end();
}

void PipelineGraph::RegisterPipeline(const Pipeline &pipeline) {
  TPL_ASSERT(!IsRegistered(pipeline), "Duplicate pipeline!");
  dependency_graph_[&pipeline] = DependencySet{};
}

void PipelineGraph::AddDependency(const Pipeline &a, const Pipeline &b) {
  TPL_ASSERT(IsRegistered(a), "Adding dependency to unregistered pipeline!");
  TPL_ASSERT(std::ranges::count(dependency_graph_[&a], &b) == 0, "Duplicate dependency!");
  dependency_graph_[&a].push_back(&b);
}

void PipelineGraph::CollectTransitiveDependencies(
    const Pipeline &pipeline, std::vector<const Pipeline *> *dependencies) const {
  TPL_ASSERT(IsRegistered(pipeline), "Provided pipeline isn't registered in graph!");
  // Use find() because this function is const.
  if (const auto iter = dependency_graph_.find(&pipeline); iter != dependency_graph_.end()) {
    for (const auto dependency : iter->second) {
      CollectTransitiveDependencies(*dependency, dependencies);
    }
    dependencies->push_back(&pipeline);
  }
  // Check all outer nestings, too.
  if (auto &outer_nestings = pipeline.GetParentPipelines(); !outer_nestings.empty()) {
    for (auto parent : outer_nestings) {
      CollectTransitiveDependencies(*parent, dependencies);
    }
  }
}

}  // namespace tpl::sql::codegen
