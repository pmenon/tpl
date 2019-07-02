#pragma once

#include <cstdint>
#include <vector>

#include "util/common.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecodes.h"

namespace tpl::vm {

class BytecodeLabel;

class BytecodeEmitter {
 public:
  /**
   * Construct a bytecode emitter instance that emits bytecode operations into
   * the provided bytecode vector
   * @param bytecode The bytecode array to emit bytecode into
   */
  explicit BytecodeEmitter(std::vector<u8> &bytecode) : bytecode_(bytecode) {}

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  /**
   * Access the current position of the emitter in the bytecode stream
   */
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
  // Special
  // -------------------------------------------------------

  // Iterate over all the states in the container
  void EmitThreadStateContainerIterate(LocalVar tls, LocalVar ctx,
                                       FunctionId iterate_fn);

  // Reset a thread state container with init and destroy functions
  void EmitThreadStateContainerReset(LocalVar tls, LocalVar state_size,
                                     FunctionId init_fn, FunctionId destroy_fn,
                                     LocalVar ctx);

  // Initialize a table iterator
  void EmitTableIterInit(Bytecode bytecode, LocalVar iter, u16 table_id);
  // Emit a parallel table scan
  void EmitParallelTableScan(u16 table_id, LocalVar ctx, LocalVar thread_states,
                             FunctionId scan_fn);

  // Reading values from an iterator
  void EmitVPIGet(Bytecode bytecode, LocalVar out, LocalVar vpi, u32 col_idx);

  // Setting values in an iterator
  void EmitVPISet(Bytecode bytecode, LocalVar vpi, LocalVar input, u32 col_idx);

  // Filter a column in the iterator by a constant value
  void EmitVPIVectorFilter(Bytecode bytecode, LocalVar selected, LocalVar vpi,
                           u32 col_idx, i64 val);

  // Insert a filter flavor into the filter manager builder
  void EmitFilterManagerInsertFlavor(LocalVar fmb, FunctionId func);

  // Lookup a single entry in the aggregation hash table
  void EmitAggHashTableLookup(LocalVar dest, LocalVar agg_ht, LocalVar hash,
                              FunctionId key_eq_fn, LocalVar arg);

  // Emit code to process a batch of input into the aggregation hash table
  void EmitAggHashTableProcessBatch(LocalVar agg_ht, LocalVar iters,
                                    FunctionId vec_hash_fn, FunctionId key_eq_fn, FunctionId vec_key_eq_fn,
                                    FunctionId init_agg_fn,
                                    FunctionId vec_merge_agg_fn,
                                    LocalVar partitioned);

  // Emit code to move thread-local data into main agg table
  void EmitAggHashTableMovePartitions(LocalVar agg_ht, LocalVar tls,
                                      LocalVar aht_offset,
                                      FunctionId merge_part_fn);

  // Emit code to scan an agg table in parallel
  void EmitAggHashTableParallelPartitionedScan(LocalVar agg_ht,
                                               LocalVar context, LocalVar tls,
                                               FunctionId scan_part_fn);

  // Initialize a sorter instance
  void EmitSorterInit(Bytecode bytecode, LocalVar sorter, LocalVar region,
                      FunctionId cmp_fn, LocalVar tuple_size);

 private:
  // Copy a scalar immediate value into the bytecode stream
  template <typename T>
  auto EmitScalarValue(const T val) -> std::enable_if_t<std::is_integral_v<T>> {
    bytecode_.insert(bytecode_.end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecode_.end() - sizeof(T))) = val;
  }

  // Emit a bytecode
  void EmitImpl(const Bytecode bytecode) {
    EmitScalarValue(Bytecodes::ToByte(bytecode));
  }

  // Emit a local variable reference by encoding it into the bytecode stream
  void EmitImpl(const LocalVar local) { EmitScalarValue(local.Encode()); }

  // Emit an integer immediate value
  template <typename T>
  auto EmitImpl(const T val) -> std::enable_if_t<std::is_integral_v<T>> {
    EmitScalarValue(val);
  }

  // Emit all arguments in sequence
  template <typename... ArgTypes>
  void EmitAll(const ArgTypes... args) {
    (EmitImpl(args), ...);
  }

  //
  void EmitJump(BytecodeLabel *label);

 private:
  std::vector<u8> &bytecode_;
};

}  // namespace tpl::vm
