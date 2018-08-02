#pragma once

#include "ast/ast.h"
#include "util/region.h"

namespace tpl {

/**
 *
 */
class AstNodeFactory {
 public:
  explicit AstNodeFactory(Region &region) : region_(region) {}

  BinaryOperation *NewBinaryOperation() {
    return new (region_) BinaryOperation(nullptr, nullptr);
  }

 private:
  Region &region_;
};

}  // namespace tpl