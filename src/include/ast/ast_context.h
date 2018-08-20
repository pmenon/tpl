#pragma once

#include "util/region.h"

namespace tpl::ast {

class Type;
class IntegerType;
class FloatType;
class PointerType;
class ArrayType;
class StructType;
class FunctionType;

class AstContext {
 public:
  explicit AstContext(util::Region &region);

  struct Implementation;
  Implementation &impl() const { return *impl_; }

  util::Region &region() const { return region_; }

 private:
  util::Region &region_;

  Implementation *impl_;
};

}  // namespace tpl::ast