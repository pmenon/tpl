#pragma once

#include <cstdint>
#include <vector>

#include "util/common.h"
#include "util/macros.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace vm {

using RegisterId = u16;

class Register {
 public:
  static constexpr const RegisterId kInvalidIndex =
      std::numeric_limits<RegisterId>::max();

  Register(std::string name, ast::Type *type, RegisterId index, size_t offset)
      : name_(std::move(name)), type_(type), id_(index), offset_(offset) {
    TPL_ASSERT(index >= 0, "Index must be positive!");
  }

  std::size_t Size() const;

  const std::string &name() const { return name_; }

  const ast::Type *type() const { return type_; }

  RegisterId id() const { return id_; }

 private:
  std::string name_;
  ast::Type *type_;
  RegisterId id_;
  size_t offset_;
};

class FunctionInfo {
 public:
  explicit FunctionInfo(u32 id) : id_(id), total_size_(0) {}

  RegisterId NewLocal(ast::Type *type, std::string name = "");

  RegisterId LookupLocal(const std::string &name);

  RegisterId GetRVRegister() const { return 0; }

  u32 id() const { return id_; }

  const std::vector<Register> &locals() const { return locals_; }

 private:
  u32 id_;
  std::vector<Register> locals_;
  size_t total_size_;
};

}  // namespace vm
}  // namespace tpl