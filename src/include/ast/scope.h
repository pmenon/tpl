#pragma once

#include "util/region.h"
#include "util/region_containers.h"

namespace tpl {
namespace ast {

class AstString;
class Declaration;

class Scope : public util::RegionObject {
 public:
  enum class Type : uint8_t { Block, Function, File };

  Scope(util::Region &region, Scope *outer, Type scope_type)
      : region_(region),
        outer_(outer),
        scope_type_(scope_type),
        declarations_(region) {}

  Declaration *Declare(const AstString *name, Declaration *decl);

  Declaration *Lookup(const AstString *name) const;
  Declaration *LookupLocal(const AstString *name) const;

 private:
  util::Region &region() const { return region_; }
  Scope *outer() const { return outer_; }

 private:
  util::Region &region_;

  Scope *outer_;

  Type scope_type_;

  util::RegionUnorderedMap<const AstString *, Declaration *> declarations_;
};

}  // namespace ast
}  // namespace tpl