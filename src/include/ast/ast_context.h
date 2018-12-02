#pragma once

#include <memory>

#include "llvm/ADT/StringRef.h"

#include "ast/builtins.h"
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

  /**
   * Return the provided string as a unique'd identifier in this context
   * @param str The input string
   * @return The equivalent unique'd string
   */
  Identifier GetIdentifier(llvm::StringRef str);

  /**
   * Is the type with name @ref identifier a builtin type?
   * @param identifier The name of the type to lookup
   * @return True if the provided name names a builtin; false otherwise
   */
  ast::Type *LookupBuiltinType(Identifier identifier) const;

  /**
   * Is the function with name @ref identifier a builtin function?
   * @param[in] identifier The name of the function to check
   * @param[out] builtin If non-null, set to the appropriate builtin enumeration
   * @return True if the function name is that of a builtin; false otherwise
   */
  bool IsBuiltinFunction(Identifier identifier,
                         Builtin *builtin = nullptr) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

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