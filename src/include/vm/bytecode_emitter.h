#pragma once

#include <cstdint>

#include "util/common.h"
#include "util/region_containers.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeLabel;

class BytecodeEmitter {
  static const u16 kJumpPlaceholder = std::numeric_limits<u16>::max() - 1u;

 public:
  explicit BytecodeEmitter(util::RegionVector<u8> &bytecode)
      : bytecode_(bytecode) {}

  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  std::size_t position() const { return bytecode_.size(); }

  template <Bytecode DerefCode>
  void EmitDeref(LocalVar dest, LocalVar src) {
    static_assert(
        DerefCode == Bytecode::Deref1 || DerefCode == Bytecode::Deref2 ||
            DerefCode == Bytecode::Deref4 || DerefCode == Bytecode::Deref8,
        "Must only call EmitDeref with scalar Deref[1|2|4|8] code");
    EmitOp(DerefCode);
    EmitLocalVars(dest, src);
  }

  void EmitDerefN(LocalVar dest, LocalVar src, u32 len) {
    EmitOp(Bytecode::DerefN);
    EmitLocalVars(dest, src);
    EmitImmediateValue(len);
  }

  template <Bytecode AssignCode>
  void EmitAssign(LocalVar dest, LocalVar src) {
    static_assert(
        AssignCode == Bytecode::Assign1 || AssignCode == Bytecode::Assign2 ||
            AssignCode == Bytecode::Assign4 || AssignCode == Bytecode::Assign8,
        "Must only call EmitDeref with scalar Deref[1|2|4|8] code");
    EmitOp(AssignCode);
    EmitLocalVars(dest, src);
  }

  void EmitAssignImm1(LocalVar dest, i8 val);
  void EmitAssignImm2(LocalVar dest, i16 val);
  void EmitAssignImm4(LocalVar dest, i32 val);
  void EmitAssignImm8(LocalVar dest, i64 val);

  void EmitJump(Bytecode bytecode, BytecodeLabel *label);
  void EmitConditionalJump(Bytecode bytecode, LocalVar cond,
                           BytecodeLabel *label);

  void EmitLea(LocalVar dest, LocalVar src, u32 offset);
  void EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, u32 scale,
                     u32 offset);

  void EmitCall(FunctionId func_id, const std::vector<LocalVar> &params);
  void EmitReturn();

  void EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input);
  void EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs,
                    LocalVar rhs);

  void Emit(Bytecode bytecode, LocalVar operand_1);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2);

  void EmitRead(Bytecode bytecode, LocalVar o1, u32 id, LocalVar o2) {
    EmitOp(bytecode);
    EmitLocalVar(o1);
    EmitImmediateValue(id);
    EmitLocalVar(o2);
  }

  template <typename T,
            typename std::enable_if_t<std::is_integral_v<T>, u32> = 0>
  void Emit(Bytecode bytecode, LocalVar operand_1, T imm) {
    EmitOp(bytecode);
    EmitLocalVar(operand_1);
    EmitImmediateValue(imm);
  }

  void Bind(BytecodeLabel *label);

 private:
  template <typename T,
            typename std::enable_if_t<std::is_integral_v<T>, u32> = 0>
  void EmitImmediateValue(T val) {
    bytecode_.insert(bytecode_.end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecode_.end() - sizeof(T))) = val;
  }

  void EmitOp(Bytecode bytecode) {
    EmitImmediateValue(Bytecodes::ToByte(bytecode));
  }

  void EmitLocalVar(LocalVar local) { EmitImmediateValue(local.Encode()); }

  template <typename... Locals,
            typename std::enable_if_t<
                std::conjunction_v<std::is_same<Locals, LocalVar>...>, u32> = 0>
  void EmitLocalVars(Locals... locals) {
    (EmitLocalVar(locals), ...);
  }

  void EmitJump(BytecodeLabel *label);

 private:
  util::RegionVector<u8> &bytecode_;
};

}  // namespace tpl::vm
