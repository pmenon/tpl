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
using FunctionId = u16;

class Register {
 public:
  static constexpr const RegisterId kInvalidIndex =
      std::numeric_limits<RegisterId>::max();

  Register(RegisterId index, std::string name, ast::Type *type,
           std::size_t offset)
      : name_(std::move(name)), type_(type), offset_(offset), id_(index) {
    TPL_ASSERT(index >= 0, "Index must be positive!");
  }

  std::size_t Size() const;

  const std::string &name() const { return name_; }

  const ast::Type *type() const { return type_; }

  RegisterId id() const { return id_; }

  std::size_t offset() const { return offset_; }

 private:
  std::string name_;
  ast::Type *type_;
  std::size_t offset_;
  RegisterId id_;
};

class FunctionInfo {
  static constexpr const RegisterId kRVRegisterId = 0;

 public:
  FunctionInfo(FunctionId id, std::string name, std::size_t bytecode_offset)
      : id_(id),
        name_(std::move(name)),
        bytecode_offset_(bytecode_offset),
        frame_size_(0) {}

  RegisterId NewLocal(ast::Type *type, std::string name = "");

  RegisterId LookupLocal(const std::string &name);

  RegisterId GetRVRegister() const { return kRVRegisterId; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  FunctionId id() const { return id_; }

  const std::string &name() const { return name_; }

  std::size_t bytecode_offset() const { return bytecode_offset_; }

  const std::vector<Register> &locals() const { return locals_; }

  std::size_t frame_size() const { return frame_size_; }

 private:
  FunctionId id_;
  std::string name_;
  std::size_t bytecode_offset_;
  std::vector<Register> locals_;
  std::size_t frame_size_;
};

}  // namespace vm
}  // namespace tpl