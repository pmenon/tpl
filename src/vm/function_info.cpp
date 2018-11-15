#include "vm/function_info.h"

#include "ast/type.h"
#include "util/math_util.h"

namespace tpl::vm {

std::size_t LocalInfo::Size() const { return type()->size(); }

LocalVar FunctionInfo::NewLocal(ast::Type *type, const std::string &name,
                                LocalInfo::Kind kind) {
  TPL_ASSERT(!name.empty(), "Local name cannot be empty");

  // Bump size to account for the alignment of the new local
  if (!util::MathUtil::IsAligned(frame_size_, type->alignment())) {
    frame_size_ = util::MathUtil::AlignTo(frame_size_, type->alignment());
  }

  auto local_id = static_cast<LocalId>(locals_.size());
  auto offset = frame_size();
  locals_.emplace_back(local_id, name, type, offset, kind);

  frame_size_ += type->size();

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::NewLocal(ast::Type *type, const std::string &name) {
  return NewLocal(type, name, LocalInfo::Kind::Var);
}

LocalVar FunctionInfo::NewParameterLocal(ast::Type *type,
                                         const std::string &name) {
  num_params_++;
  return NewLocal(type, name, LocalInfo::Kind::Parameter);
}

LocalVar FunctionInfo::NewTempLocal(ast::Type *type) {
  std::string tmp_name = "tmp" + std::to_string(NextTempId());
  return NewLocal(type, tmp_name, LocalInfo::Kind::Temporary);
}

LocalVar FunctionInfo::LookupLocal(const std::string &name) {
  for (const auto &local_info : locals()) {
    if (local_info.name() == name) {
      return LocalVar(local_info.offset(), LocalVar::AddressMode::Address);
    }
  }

  // Invalid local
  return LocalVar();
}

}  // namespace tpl::vm