#include "plan_node/seq_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::plan_node {

common::hash_t SeqScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  hash = common::HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  // Hash is parallel
  auto is_parallel = IsParallel();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_parallel));

  // Hash is_for_update
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool SeqScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &rhs_plan_node = static_cast<const SeqScanPlanNode &>(rhs);

  // Predicate
  auto *pred = GetPredicate();
  auto *rhs_plan_node_pred = rhs_plan_node.GetPredicate();
  if ((pred == nullptr && rhs_plan_node_pred != nullptr) || (pred != nullptr && rhs_plan_node_pred == nullptr))
    return false;
  if (pred != nullptr && *pred != *rhs_plan_node_pred) return false;

  if (IsParallel() != rhs_plan_node.IsParallel()) return false;

  if (IsForUpdate() != rhs_plan_node.IsForUpdate()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
