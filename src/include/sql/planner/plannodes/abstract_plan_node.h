#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "sql/planner/plannodes/output_schema.h"
#include "sql/planner/plannodes/plan_node_defs.h"

namespace tpl::sql::planner {

/**
 * An abstract plan node should be the base class for (almost) all plan nodes
 */
class AbstractPlanNode {
 protected:
  /**
   * Base builder class for plan nodes
   * @tparam ConcreteType
   */
  template <class ConcreteType>
  class Builder {
   public:
    Builder() = default;
    virtual ~Builder() = default;

    /**
     * @param child child to be added
     * @return builder object
     */
    ConcreteType &AddChild(std::unique_ptr<AbstractPlanNode> child) {
      children_.emplace_back(std::move(child));
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param output_schema output schema for plan node
     * @return builder object
     */
    ConcreteType &SetOutputSchema(std::unique_ptr<OutputSchema> output_schema) {
      output_schema_ = std::move(output_schema);
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    /**
     * child plans
     */
    std::vector<std::unique_ptr<AbstractPlanNode>> children_;
    /**
     * schema describing output of the node
     */
    std::unique_ptr<OutputSchema> output_schema_{nullptr};
  };

  /**
   * Constructor for the base AbstractPlanNode. Derived plan nodes should call this constructor to
   * set output_schema
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  AbstractPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                   std::unique_ptr<OutputSchema> output_schema)
      : children_(std::move(children)), output_schema_(std::move(output_schema)) {}

 public:
  /**
   * Constructor for Deserialization and DDL statements
   */
  AbstractPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AbstractPlanNode)

  virtual ~AbstractPlanNode() = default;

  //===--------------------------------------------------------------------===//
  // Children Helpers
  //===--------------------------------------------------------------------===//

  /**
   * @return child plan nodes
   */
  std::vector<const AbstractPlanNode *> GetChildren() const {
    std::vector<const AbstractPlanNode *> children;
    children.reserve(children_.size());
    for (const auto &child : children_) {
      children.emplace_back(child.get());
    }
    return children;
  }

  /**
   * @return number of children
   */
  size_t GetChildrenSize() const { return children_.size(); }

  /**
   * @param child_index index of child
   * @return child at provided index
   */
  const AbstractPlanNode *GetChild(uint32_t child_index) const {
    TPL_ASSERT(child_index < children_.size(),
               "index into children of plan node should be less than number of children");
    return children_[child_index].get();
  }

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  /**
   * Returns plan type, each derived plan class should override this method to return their specific
   * type
   * @return plan type
   */
  virtual PlanNodeType GetPlanNodeType() const = 0;

  /**
   * @return output schema for the node. The output schema contains information on columns of the
   * output of the plan node operator
   */
  const OutputSchema *GetOutputSchema() const { return output_schema_.get(); }

 private:
  std::vector<std::unique_ptr<AbstractPlanNode>> children_;
  std::unique_ptr<OutputSchema> output_schema_;
};

}  // namespace tpl::sql::planner
