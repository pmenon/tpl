#pragma once

#include <memory>

#include "llvm/ADT/StringRef.h"

#include "ast/identifier.h"
#include "util/region.h"

namespace tpl {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace ast {

class AstNodeFactory;
class Type;

class AstContext {
 public:
  explicit AstContext(util::Region *region,
                      sema::ErrorReporter &error_reporter);

  DISALLOW_COPY_AND_MOVE(AstContext);

  ~AstContext();

  Identifier GetIdentifier(llvm::StringRef str);

  ast::Type *LookupBuiltin(Identifier identifier);

  struct Implementation;
  Implementation &impl() const { return *impl_; }

  ast::AstNodeFactory &node_factory() const { return *node_factory_; }

  sema::ErrorReporter &error_reporter() const { return error_reporter_; }

  util::Region *region() const { return region_; }

 private:
  // Region allocator for all Ast objects this context needs
  util::Region *region_;

  // Error reporter
  sema::ErrorReporter &error_reporter_;

  // The factory used for Ast nodes
  std::unique_ptr<ast::AstNodeFactory> node_factory_;

  // Pimpl
  std::unique_ptr<Implementation> impl_;
};

}  // namespace ast
}  // namespace tpl