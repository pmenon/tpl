#pragma once

#include "util/common.h"
#include "util/region_containers.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class LocalVar;

class BytecodeIterator {
 public:
  BytecodeIterator(const util::RegionVector<u8> &bytecode, std::size_t start,
                   std::size_t end);

  /// Get the bytecode instruction the iterator is currently pointing to
  /// \return The current bytecode instruction
  Bytecode CurrentBytecode() const;

  /// Advance the iterator by one bytecode instruction. It's expected the caller
  /// has verified there are more instructions with a preceding call to \link
  /// Done()
  void Advance();

  /// Has the iterator reached the end?
  /// \return True if complete; false otherwise
  bool Done() const;

  /// Read the operand at index \a operand_index for the current bytecode as a
  /// signed immediate value
  /// \param operand_index The index of operand to retrieve
  /// \return The immediate value, up-casted to a signed 64-bit integer
  i64 GetImmediateOperand(u32 operand_index) const;

  /// Read the operand at index \a operand_index for the current bytecode as an
  /// unsigned immediate value
  /// \param operand_index The index of the operand to retrieve
  /// \return The immediate value, up-casted to an unsigned 64-bit integer
  u64 GetUnsignedImmediateOperand(u32 operand_index) const;

  /// Read the operand at index \a operand_index for the current bytecode as a
  /// jump offset as part of either a conditional or unconditional jump
  /// \param operand_index The index of the operand to retrieve
  /// \return The jump offset at the given index
  i32 GetJumpOffsetOperand(u32 operand_index) const;

  /// Get the operand at index \a operand_index for the current bytecode
  /// \param operand_index The index of the operand to retrieve
  /// \return The operand at the given operand index
  LocalVar GetLocalOperand(u32 operand_index) const;

  /// Get the operand at \a operand_index for the current bytecode as a count of
  /// local variables appearing after this operand
  /// \param operand_index The index of the operand to retrieve
  /// \return The number of operands
  u16 GetLocalCountOperand(u32 operand_index) const;

  /// Return the total size in bytes of the bytecode instruction the iterator is
  /// currently pointing to. This size includes variable length arguments.
  /// \return
  u32 CurrentBytecodeSize() const;

  ///
  /// \param offset
  void SetOffset(std::size_t offset) {
    //TPL_ASSERT(offset < (end_offset() - start_offset()), "Invalid offset");
    curr_offset_ = offset;
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  std::size_t start_offset() const { return start_offset_; }

  std::size_t end_offset() const { return end_offset_; }

  std::size_t current_offset() const { return curr_offset_ - start_offset_; }

  const util::RegionVector<u8> &bytecodes() const { return bytecodes_; }

 private:
  // Return the address of the first bytecode we're allowed to see
  const u8 *GetFirstBytecodeAddress() const {
    return bytecodes().data() + start_offset();
  }

 private:
  // ALL the bytecode instructions for a TPL compilation unit
  const util::RegionVector<u8> &bytecodes_;
  std::size_t start_offset_;
  std::size_t end_offset_;
  std::size_t curr_offset_;
};

}  // namespace tpl::vm