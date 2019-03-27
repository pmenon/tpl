#include "vm/bytecode_function_info.h"

#include "ast/type.h"
#include "util/math_util.h"

namespace tpl::vm {

// ---------------------------------------------------------
// Local Information
// ---------------------------------------------------------

LocalInfo::LocalInfo(std::string name, ast::Type *type, u32 offset,
                     LocalInfo::Kind kind) noexcept
    : name_(std::move(name)),
      type_(type),
      offset_(offset),
      size_(type->size()),
      kind_(kind) {}

// ---------------------------------------------------------
// Function Information
// ---------------------------------------------------------

LocalVar FunctionInfo::NewLocal(ast::Type *type, const std::string &name,
                                LocalInfo::Kind kind) {
  TPL_ASSERT(!name.empty(), "Local name cannot be empty");

  // Bump size to account for the alignment of the new local
  if (!util::MathUtil::IsAligned(frame_size_, type->alignment())) {
    frame_size_ = util::MathUtil::AlignTo(frame_size_, type->alignment());
  }

  auto offset = static_cast<u32>(frame_size());
  locals_.emplace_back(name, type, offset, kind);

  frame_size_ += type->size();

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::NewLocal(ast::Type *type, const std::string &name) {
  if (name.empty()) {
    const auto tmp_name = "tmp" + std::to_string(++num_temps_);
    return NewLocal(type, tmp_name, LocalInfo::Kind::Var);
  }

  return NewLocal(type, name, LocalInfo::Kind::Var);
}

LocalVar FunctionInfo::NewParameterLocal(ast::Type *type,
                                         const std::string &name) {
  num_params_++;
  return NewLocal(type, name, LocalInfo::Kind::Parameter);
}

LocalVar FunctionInfo::LookupLocal(const std::string &name) const {
  for (const auto &local_info : locals()) {
    if (local_info.name() == name) {
      return LocalVar(local_info.offset(), LocalVar::AddressMode::Address);
    }
  }

  // Invalid local
  return LocalVar();
}

const LocalInfo *FunctionInfo::LookupLocalInfo(u32 offset) const {
  for (const auto &local_info : locals()) {
    if (local_info.offset() == offset) {
      return &local_info;
    }
  }

  // Invalid local
  return nullptr;
}

void FunctionInfo::GetParameterInfos(
    std::vector<const LocalInfo *> &params) const {
  params.clear();
  for (const auto &local_info : locals()) {
    if (local_info.is_parameter()) {
      params.push_back(&local_info);
    }
  }
}

}  // namespace tpl::vm
