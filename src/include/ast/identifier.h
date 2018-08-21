#pragma once

#include "util/string_ref.h"

namespace tpl::ast {

/**
 * A uniqued string identifier
 */
class Identifier : public util::StringRef {
 public:
  // These aren't explicit on purpose
  Identifier() noexcept : util::StringRef() {}
  Identifier(nullptr_t) noexcept : util::StringRef() {}
  Identifier(const char *str) noexcept : util::StringRef(str) {}
  Identifier(const char *str, uint64_t len) noexcept
      : util::StringRef(str, len) {}
  Identifier(const std::string &str) noexcept : util::StringRef(str) {}

  bool operator==(const Identifier &other) const {
    return data() == other.data();
  }

  bool operator!=(const Identifier &other) const { return !(*this == other); }
};

struct IdentifierHasher {
  size_t operator()(const Identifier &identifier) const {
    return std::hash<const void *>()(identifier.data());
  }
};

struct IdentifierEquality {
  bool operator()(const Identifier &lhs, const Identifier &rhs) const {
    return lhs == rhs;
  }
};

}  // namespace tpl::ast