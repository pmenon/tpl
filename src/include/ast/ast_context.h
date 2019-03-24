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

  /// Return \a str as a unique string in this context
  Identifier GetIdentifier(llvm::StringRef str);

  /// Is the type with name \a identifier a builtin type?
  /// \return A non-null pointer to the Type if a valid builtin; null otherwise
  ast::Type *LookupBuiltinType(Identifier identifier) const;

  /// Is the type with name \a identifier an internal type?
  /// \return A non-null pointer to the Type if an internal type; null otherwise
  ast::Type *LookupInternalType(Identifier identifier) const;

  /// Is the function with name \a identifier a builtin function?
  /// \param[in] identifier The name of the function to check
  /// \param[out] builtin Set to the appropriate builtin enumeration if non-null
  /// \return True if the function name is that of a builtin; false otherwise
  bool IsBuiltinFunction(Identifier identifier,
                         Builtin *builtin = nullptr) const;

  // -------------------------------------------------------
  // Simple accessors
  // -------------------------------------------------------

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
