#pragma once

#include "ast/identifier.h"
#include "util/region.h"
#include "util/string_ref.h"

namespace tpl {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace ast {

class ArrayType;
class AstNodeFactory;
class FloatType;
class FunctionType;
class IntegerType;
class PointerType;
class StructType;
class Type;

class AstContext {
 public:
  explicit AstContext(util::Region &region, ast::AstNodeFactory &node_factory,
                      sema::ErrorReporter &error_reporter);

  Identifier GetIdentifier(util::StringRef str);

  ast::Type *LookupBuiltin(Identifier identifier);

  struct Implementation;
  Implementation &impl() const { return *impl_; }

  ast::AstNodeFactory &node_factory() const { return node_factory_; }

  sema::ErrorReporter &error_reporter() const { return error_reporter_; }

  util::Region &region() const { return region_; }

 private:
  util::Region &region_;

  ast::AstNodeFactory &node_factory_;

  sema::ErrorReporter &error_reporter_;

  Implementation *impl_;
};

}  // namespace ast
}  // namespace tpl