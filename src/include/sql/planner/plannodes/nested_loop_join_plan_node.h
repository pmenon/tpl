#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "sql/planner/plannodes/abstract_join_plan_node.h"

namespace tpl::sql::planner {

/**
 * Plan node for nested loop joins
 */
class NestedLoopJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for nested loop join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Build the nested loop join plan node
     * @return plan node
     */
    std::shared_ptr<NestedLoopJoinPlanNode> Build() {
      return std::shared_ptr<NestedLoopJoinPlanNode>(new NestedLoopJoinPlanNode(
          std::move(children_), std::move(output_schema_), join_type_, join_predicate_));
    }
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  NestedLoopJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                         std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                         const AbstractExpression *predicate)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate) {}

 public:
  DISALLOW_COPY_AND_MOVE(NestedLoopJoinPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::NESTLOOP; }
};

}  // namespace tpl::sql::planner
