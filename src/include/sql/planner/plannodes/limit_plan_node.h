#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::planner {

/**
 * Plan node for a limit operator.
 */
class LimitPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for limit plan node.
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param limit number of tuples to limit to
     * @return object
     */
    Builder &SetLimit(std::size_t limit) {
      limit_ = limit;
      return *this;
    }

    /**
     * @param offset offset for where to limit from
     * @return builder object
     */
    Builder &SetOffset(std::size_t offset) {
      offset_ = offset;
      return *this;
    }

    /**
     * @return The constructed limit plan.
     */
    std::unique_ptr<LimitPlanNode> Build() {
      return std::unique_ptr<LimitPlanNode>(
          new LimitPlanNode(std::move(children_), std::move(output_schema_), limit_, offset_));
    }

   protected:
    // The limit.
    std::size_t limit_;
    // The offset.
    std::size_t offset_;
  };

 private:
  /**
   * @param children child plan nodes.
   * @param output_schema Schema representing the structure of the output of this plan node.
   * @param limit number of tuples to limit to.
   * @param offset offset at which to limit from.
   */
  LimitPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                std::unique_ptr<OutputSchema> output_schema, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        limit_(limit),
        offset_(offset) {}

 public:
  DISALLOW_COPY_AND_MOVE(LimitPlanNode)

  /**
   * @return The type of this plan.
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  /**
   * @return The limit value.
   */
  std::size_t GetLimit() const { return limit_; }

  /**
   * @return The offset value.
   */
  std::size_t GetOffset() const { return offset_; }

 private:
  // The limit.
  std::size_t limit_;
  // The offset.
  std::size_t offset_;
};

}  // namespace tpl::sql::planner
