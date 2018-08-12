#pragma once

#include "util/region_containers.h"

namespace tpl {

namespace util {
class Region;
}  // namespace util

namespace ast {

class AstString;
class Declaration;

class Scope {
 public:
  Scope(util::Region &region, Scope *outer)
      : region_(region), outer_(outer), declarations_(region) {}

  Declaration *Lookup(const AstString *name) const;
  Declaration *LookupLocal(const AstString *name) const;

  util::Region &region() const { return region_; }

  Scope *outer() const { return outer_; }

 private:
  util::Region &region_;

  Scope *outer_;

  util::RegionUnorderedMap<const AstString *, Declaration *> declarations_;
};

}  // namespace ast
}  // namespace tpl