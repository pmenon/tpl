#pragma once

#include "util/region.h"
#include "util/region_containers.h"

namespace tpl {

namespace ast {
class AstString;
class Type;
}  // namespace ast

namespace sema {

class Scope : public util::RegionObject {
 public:
  enum class Kind : uint8_t { Block, Function, File };

  Scope(util::Region &region, Scope *outer, Kind scope_kind)
      : region_(region),
        outer_(outer),
        scope_kind_(scope_kind),
        table_(region) {}

  // Declare an element with the given name and type in this scope. Return true
  // if successful and false if an element with the given name already exits in
  // the local scope.
  bool Declare(const ast::AstString *name, ast::Type *type);

  ast::Type *Lookup(const ast::AstString *name) const;
  ast::Type *LookupLocal(const ast::AstString *name) const;

  Kind scope_kind() const { return scope_kind_; }

 private:
  util::Region &region() const { return region_; }

  Scope *outer() const { return outer_; }

 private:
  util::Region &region_;

  Scope *outer_;

  Kind scope_kind_;

  util::RegionUnorderedMap<const ast::AstString *, ast::Type *> table_;
};

}  // namespace sema
}  // namespace tpl