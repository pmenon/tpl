#include "vm/bytecode_register.h"

#include "ast/type.h"
#include "util/math_util.h"

namespace tpl::vm {

std::size_t Register::Size() const { return type()->size(); }

RegisterId FunctionInfo::NewLocal(ast::Type *type, const std::string &name,
                                  bool is_param) {
  TPL_ASSERT(!name.empty(), "Local name cannot be empty");

  // Bump size to account for the alignment of the new local
  if (!util::MathUtil::IsAligned(frame_size_, type->alignment())) {
    frame_size_ = util::MathUtil::AlignTo(frame_size_, type->alignment());
  }

  auto register_id = static_cast<RegisterId>(locals_.size());
  auto offset = frame_size();
  locals_.emplace_back(register_id, name, type, offset, is_param);

  frame_size_ += type->size();

  return register_id;
}

RegisterId FunctionInfo::NewLocal(ast::Type *type) {
  return NewLocal(type, "tmp" + std::to_string(NextTempRegId()), false);
}

RegisterId FunctionInfo::LookupLocal(const std::string &name) {
  // TODO(pmenon): More efficient lookup?
  for (const auto &reg : locals()) {
    if (reg.name() == name) {
      return reg.id();
    }
  }
  return Register::kInvalidIndex;
}

}  // namespace tpl::vm