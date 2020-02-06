#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/plannodes/abstract_plan_node.h"

namespace tpl::sql::planner {

/**
 * Base class for sql scans
 */
class AbstractScanPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Base builder class for scan plan nodes
   * @tparam ConcreteType
   */
  template <class ConcreteType>
  class Builder : public AbstractPlanNode::Builder<ConcreteType> {
   public:
    /**
     * @param predicate predicate to use for scan
     * @return builder object
     */
    ConcreteType &SetScanPredicate(const AbstractExpression *predicate) {
      scan_predicate_ = predicate;
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    /**
     * Scan predicate
     */
    const AbstractExpression *scan_predicate_;
  };

  /**
   * Base constructor for scans. Derived scan plans should call this constructor
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   */
  AbstractScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::unique_ptr<OutputSchema> output_schema,
                       const AbstractExpression *predicate)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        scan_predicate_(predicate) {}

 public:
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode)

  /**
   * @return predicate used for performing scan
   */
  const AbstractExpression *GetScanPredicate() const { return scan_predicate_; }

 private:
  /**
   * Selection predicate.
   */
  const AbstractExpression *scan_predicate_;
};

}  // namespace tpl::sql::planner
