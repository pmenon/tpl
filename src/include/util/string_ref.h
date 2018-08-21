#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace tpl::util {

class StringRef {
 public:
  using iterator = const char *;
  using const_iterator = const char *;

  // Implicit constructors allowed here!
  StringRef() = default;
  StringRef(nullptr_t) = delete;
  StringRef(const char *str)
      : data_(str), len_(str != nullptr ? std::strlen(str) : 0) {}
  StringRef(const char *str, uint64_t len) : data_(str), len_(len) {}
  StringRef(const std::string &str) : data_(str.data()), len_(str.length()) {}

  const char *data() const { return data_; }

  uint64_t length() const { return len_; }

  bool empty() const { return length() == 0; }

  iterator begin() { return data(); }
  iterator end() { return data() + length(); }

  bool equals(const StringRef other) const {
    return (length() == other.length() &&
        std::memcmp(data(), other.data(), length()) == 0);
  }

  bool operator==(const StringRef other) const { return equals(other); }

 private:
  const char *data_;
  uint64_t len_;
};

} //