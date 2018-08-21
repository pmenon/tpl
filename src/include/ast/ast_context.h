#pragma once

#include "ast/identifier.h"
#include "util/region.h"
#include "util/string_ref.h"

namespace tpl {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace ast {

class Type;
class IntegerType;
class FloatType;
class PointerType;
class ArrayType;
class StructType;
class FunctionType;

class AstContext {
 public:
  explicit AstContext(util::Region &region,
                      sema::ErrorReporter &error_reporter);

  Identifier GetIdentifier(util::StringRef str);

  struct Implementation;
  Implementation &impl() const { return *impl_; }

  sema::ErrorReporter &error_reporter() const { return error_reporter_; }

  util::Region &region() const { return region_; }

 private:
  util::Region &region_;

  sema::ErrorReporter &error_reporter_;

  Implementation *impl_;
};

}  // namespace ast
}  // namespace tpl