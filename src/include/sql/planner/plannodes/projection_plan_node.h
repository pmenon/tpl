#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::planner {

/**
 * Plan node for projections.
 */
class ProjectionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for projection plan node.
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved.
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @return The constructed projection plan node.
     */
    std::unique_ptr<ProjectionPlanNode> Build() {
      return std::unique_ptr<ProjectionPlanNode>(
          new ProjectionPlanNode(std::move(children_), std::move(output_schema_)));
    }
  };

 private:
  // Create a new projection plan. Private to force use of builder.
  explicit ProjectionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                              std::unique_ptr<OutputSchema> output_schema)
      : AbstractPlanNode(std::move(children), std::move(output_schema)) {}

 public:
  DISALLOW_COPY_AND_MOVE(ProjectionPlanNode)

  /**
   * @return The type of this plan node.
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::PROJECTION; }
};

}  // namespace tpl::sql::planner
