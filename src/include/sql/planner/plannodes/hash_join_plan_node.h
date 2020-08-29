#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "sql/planner/plannodes/abstract_join_plan_node.h"

namespace tpl::sql::planner {

/**
 * Plan node for hash join. Hash joins are constructed so that the left is the probe table, and the
 right is the hashed
 * table
 */
class HashJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for hash join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param key key to add to left hash keys
     * @return builder object
     */
    Builder &AddLeftHashKey(const AbstractExpression *key) {
      left_hash_keys_.emplace_back(key);
      return *this;
    }

    /**
     * @param key key to add to right hash keys
     * @return builder object
     */
    Builder &AddRightHashKey(const AbstractExpression *key) {
      right_hash_keys_.emplace_back(key);
      return *this;
    }

    // TODO(WAN) do we want to invalidate the builder after build?
    /**
     * Build the hash join plan node
     * @return plan node
     */
    std::unique_ptr<HashJoinPlanNode> Build() {
      return std::unique_ptr<HashJoinPlanNode>(new HashJoinPlanNode(
          std::move(children_), std::move(output_schema_), join_type_, join_predicate_,
          std::move(left_hash_keys_), std::move(right_hash_keys_)));
    }

   protected:
    /**
     * left side hash keys
     */
    std::vector<const AbstractExpression *> left_hash_keys_;
    /**
     * right side hash keys
     */
    std::vector<const AbstractExpression *> right_hash_keys_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   * @param left_hash_keys left side keys to be hashed on
   * @param right_hash_keys right side keys to be hashed on
   * @param build_bloomfilter flag whether to build a bloom filter
   */
  HashJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                   std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                   const AbstractExpression *predicate,
                   std::vector<const AbstractExpression *> &&left_hash_keys,
                   std::vector<const AbstractExpression *> &&right_hash_keys)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate),
        left_hash_keys_(std::move(left_hash_keys)),
        right_hash_keys_(std::move(right_hash_keys)) {}

 public:
  DISALLOW_COPY_AND_MOVE(HashJoinPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASHJOIN; }

  /**
   * @return left side hash keys
   */
  const std::vector<const AbstractExpression *> &GetLeftHashKeys() const { return left_hash_keys_; }

  /**
   * @return right side hash keys
   */
  const std::vector<const AbstractExpression *> &GetRightHashKeys() const {
    return right_hash_keys_;
  }

 private:
  // The left and right expressions that constitute the join keys
  std::vector<const AbstractExpression *> left_hash_keys_;
  std::vector<const AbstractExpression *> right_hash_keys_;
};

}  // namespace tpl::sql::planner
