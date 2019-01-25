#pragma once

#include <cstdint>

#include "util/common.h"
#include "util/region_containers.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeLabel;

class BytecodeEmitter {
 public:
  /// Construct a bytecode emitter instance that emits bytecode operations into
  /// the provided bytecode vector
  explicit BytecodeEmitter(util::RegionVector<u8> &bytecode) noexcept
      : bytecode_(bytecode) {}

  /// Cannot copy or move this class
  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  /// Access the current position of the emitter in the bytecode stream
  std::size_t position() const { return bytecode_.size(); }

  // -------------------------------------------------------
  // Derefs
  // -------------------------------------------------------

  void EmitDeref(Bytecode bytecode, LocalVar dest, LocalVar src);
  void EmitDerefN(LocalVar dest, LocalVar src, u32 len);

  // -------------------------------------------------------
  // Assignment
  // -------------------------------------------------------

  void EmitAssign(Bytecode bytecode, LocalVar dest, LocalVar src);
  void EmitAssignImm1(LocalVar dest, i8 val);
  void EmitAssignImm2(LocalVar dest, i16 val);
  void EmitAssignImm4(LocalVar dest, i32 val);
  void EmitAssignImm8(LocalVar dest, i64 val);

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  // Bind the given label to the current bytecode position
  void Bind(BytecodeLabel *label);

  void EmitJump(Bytecode bytecode, BytecodeLabel *label);
  void EmitConditionalJump(Bytecode bytecode, LocalVar cond,
                           BytecodeLabel *label);

  // -------------------------------------------------------
  // Load-effective-address
  // -------------------------------------------------------

  void EmitLea(LocalVar dest, LocalVar src, u32 offset);
  void EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, u32 scale,
                     u32 offset);

  // -------------------------------------------------------
  // Calls and returns
  // -------------------------------------------------------

  void EmitCall(FunctionId func_id, const std::vector<LocalVar> &params);
  void EmitReturn();

  // -------------------------------------------------------
  // Generic unary and binary operations
  // -------------------------------------------------------

  void EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input);
  void EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs,
                    LocalVar rhs);

  // -------------------------------------------------------
  // Generic emissions
  // -------------------------------------------------------

  void Emit(Bytecode bytecode, LocalVar operand_1);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5,
            LocalVar operand_6);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5,
            LocalVar operand_6, LocalVar operand_7);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5,
            LocalVar operand_6, LocalVar operand_7, LocalVar operand_8);

  // -------------------------------------------------------
  // Tables
  // -------------------------------------------------------

  void EmitTableIteratorInit(Bytecode bytecode, LocalVar iter, u16 table_id);

  // Reading integer values from an iterator
  void EmitVPIGet(Bytecode bytecode, LocalVar out, LocalVar vpi, u32 col_idx);

  // Filter a column in the iterator by a constant value
  void EmitVPIVectorFilter(Bytecode bytecode, LocalVar selected, LocalVar iter,
                           u32 col_idx, i64 val);

 private:
  // Copy a scalar immediate value into the bytecode stream
  template <typename T>
  typename std::enable_if_t<std::is_integral_v<T>> EmitScalarValue(T val) {
    bytecode_.insert(bytecode_.end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecode_.end() - sizeof(T))) = val;
  }

  // Emit a bytecode
  void EmitImpl(Bytecode bytecode) {
    EmitScalarValue(Bytecodes::ToByte(bytecode));
  }

  // Emit a local variable reference by encoding it into the bytecode stream
  void EmitImpl(LocalVar local) { EmitScalarValue(local.Encode()); }

  // Emit an integer immediate value
  template <typename T>
  typename std::enable_if_t<std::is_integral_v<T>> EmitImpl(T val) {
    EmitScalarValue(val);
  }

  // Emit all arguments in sequence
  template <typename... ArgTypes>
  void EmitAll(ArgTypes... args) {
    (EmitImpl(args), ...);
  }

  //
  void EmitJump(BytecodeLabel *label);

 private:
  util::RegionVector<u8> &bytecode_;
};

}  // namespace tpl::vm
