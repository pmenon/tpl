#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/planner/plannodes/abstract_scan_plan_node.h"

namespace tpl::sql::planner {

/**
 * Plan node for sequential table scan
 */
class SeqScanPlanNode : public AbstractScanPlanNode {
 public:
  /**
   * Builder for a sequential scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param oid oid for table to scan
     * @return builder object
     */
    Builder &SetTableOid(uint16_t oid) {
      table_oid_ = oid;
      return *this;
    }

    /**
     * Build the sequential scan plan node
     * @return plan node
     */
    std::unique_ptr<SeqScanPlanNode> Build() {
      return std::unique_ptr<SeqScanPlanNode>(new SeqScanPlanNode(
          std::move(children_), std::move(output_schema_), scan_predicate_, table_oid_));
    }

   protected:
    /**
     * OID for table being scanned
     */
    uint16_t table_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate scan predicate
   * @param table_oid OID for table to scan
   */
  SeqScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::unique_ptr<OutputSchema> output_schema, const AbstractExpression *predicate,
                  uint16_t table_oid)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), predicate),
        table_oid_(table_oid) {}

 public:
  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  /**
   * @return the OID for the table being scanned
   */
  uint16_t GetTableOid() const { return table_oid_; }

 private:
  /**
   * OID for table being scanned
   */
  uint16_t table_oid_;
};

}  // namespace tpl::sql::planner
