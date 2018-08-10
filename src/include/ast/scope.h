#pragma once

namespace tpl {

class AstString;
class Region;

class Scope {
 public:
  Scope(Region &region, Scope *outer) : region_(region), outer_(outer) {}

  explicit Scope(Region &region) : Scope(region, nullptr) {}

  void Lookup(const AstString *name) const;
  void LookupLocal(const AstString *name) const;

  Region &region() const { return region_; }

  Scope *outer() const { return outer_; }

 private:
  Region &region_;

  Scope *outer_;
};

}  // namespace tpl