#pragma once

#include <cstdint>
#include <functional>

namespace tpl::ast {

/**
 * A uniqued string identifier in some AST context. This serves as a super-
 * lightweight string reference. Two identifiers are equal if they point to the
 * same string (i.e., we don't need to check contents).
 */
class Identifier {
 public:
  // These aren't explicit on purpose
  Identifier() noexcept : data_(nullptr), len_(0) {}
  Identifier(nullptr_t) noexcept : Identifier() {}
  Identifier(const char *str, size_t len) noexcept : data_(str), len_(len) {}
  Identifier(const std::string &str) noexcept
      : data_(str.data()), len_(str.length()) {}

  const char *data() const { return data_; }

  size_t length() const { return len_; }

  bool empty() const { return len_ == 0; }

  bool operator==(const Identifier &other) const {
    return data() == other.data();
  }

  bool operator!=(const Identifier &other) const { return !(*this == other); }

 private:
  const char *data_;
  std::size_t len_;
};

struct IdentifierHasher {
  std::size_t operator()(const Identifier &identifier) const {
    return std::hash<const void *>()(identifier.data());
  }
};

struct IdentifierEquality {
  bool operator()(const Identifier &lhs, const Identifier &rhs) const {
    return lhs == rhs;
  }
};

}  // namespace tpl::ast