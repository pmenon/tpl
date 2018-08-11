#pragma once

#include "util/region_containers.h"

namespace tpl {

class AstString;
class Region;
class Declaration;

class Scope {
 public:
  Scope(Region &region, Scope *outer)
      : region_(region), outer_(outer), declarations_(region) {}

  explicit Scope(Region &region) : Scope(region, nullptr) {}

  Declaration *Lookup(const AstString *name) const;
  Declaration *LookupLocal(const AstString *name) const;

  Region &region() const { return region_; }

  Scope *outer() const { return outer_; }

 private:
  Region &region_;

  Scope *outer_;

  util::RegionUnorderedMap<const AstString *, Declaration *> declarations_;
};

}  // namespace tpl