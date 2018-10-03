#include "vm/bytecode_register.h"

#include "ast/type.h"
#include "util/math_util.h"

namespace tpl::vm {

std::size_t Register::Size() const { return type()->size(); }

RegisterId FunctionInfo::NewLocal(ast::Type *type, std::string name) {
  // Bump size to account for the alignment of the new local
  if (!util::MathUtil::IsAligned(frame_size_, type->alignment())) {
    frame_size_ = util::MathUtil::AlignTo(frame_size_, type->alignment());
  }

  std::string reg_name = name;
  if (name.empty()) {
    reg_name = "tmp" + std::to_string(NextTempRegId());
  }

  auto arg_id = static_cast<RegisterId>(locals_.size());
  auto offset = frame_size_;
  locals_.emplace_back(arg_id, reg_name , type, offset);

  frame_size_ += type->size();

  return arg_id;
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