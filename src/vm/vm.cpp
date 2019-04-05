#include "vm/vm.h"

#include <numeric>
#include <string>
#include <vector>

#include "sql/table.h"
#include "sql/value.h"
#include "util/common.h"
#include "util/timer.h"
#include "vm/bytecode_function_info.h"
#include "vm/bytecode_handlers.h"
#include "vm/bytecode_module.h"

namespace tpl::vm {

// ---------------------------------------------------------
// Virtual Machine Frame
// ---------------------------------------------------------

/// An execution frame where all function's local variables and parameters live
/// for the duration of the function's lifetime.
class VM::Frame {
  friend class VM;

 public:
  /// Constructor
  Frame(VM *vm, std::size_t frame_size) : vm_(vm), frame_size_(frame_size) {
    frame_data_ = vm->AllocateFrame(frame_size);
    TPL_ASSERT(frame_data_ != nullptr, "Frame data cannot be null");
    TPL_ASSERT(frame_size_ >= 0, "Frame size must be >= 0");
  }

  /// Destructor
  ~Frame() { vm_->ReleaseFrame(frame_size_); }

  /// Access the local variable at the given index in the fame. The \ref 'index'
  /// attribute is encoded and indicates whether the local variable is accessed
  /// through an indirection (i.e., if the variable has to be dereferenced or
  /// loaded)
  /// \tparam T The type of the variable the user expects
  /// \param index The encoded index into the frame where the variable is
  /// \return The value of the variable. Note that this is copied!
  template <typename T>
  T LocalAt(u32 index) const {
    LocalVar local = LocalVar::Decode(index);

    EnsureInFrame(local);

    auto val = reinterpret_cast<uintptr_t>(&frame_data_[local.GetOffset()]);

    if (local.GetAddressMode() == LocalVar::AddressMode::Value) {
      return *reinterpret_cast<T *>(val);
    }

    return (T)(val);  // NOLINT (both static/reinterpret cast semantics)
  }

 private:
#ifndef NDEBUG
  // Ensure the local variable is valid
  void EnsureInFrame(LocalVar var) const {
    if (var.GetOffset() >= frame_size_) {
      std::string error_msg =
          fmt::format("Accessing local at offset {}, beyond frame of size {}",
                      var.GetOffset(), frame_size_);
      LOG_ERROR("{}", error_msg);
      throw std::runtime_error(error_msg);
    }
  }
#else
  void EnsureInFrame(UNUSED LocalVar var) const {}
#endif

  u8 *raw_frame() const { return frame_data_; }

 private:
  VM *vm_;
  u8 *frame_data_;
  std::size_t frame_size_;
};

// ---------------------------------------------------------
// Virtual Machine
// ---------------------------------------------------------

VM::VM(const BytecodeModule &module, util::Region *region)
    : our_region_(module.name()),
      region_(region != nullptr ? region : &our_region_),
      stack_(kDefaultInitialStackSize, 0, region_),
      sp_(0),
      module_(module),
      bytecode_counts_{0ull} {
  (void)bytecode_counts_;
}  // unused variable

// static
void VM::InvokeFunction(const BytecodeModule &module, const FunctionId func,
                        const u8 args[]) {
  const FunctionInfo *const func_info = module.GetFuncInfoById(func);
  const u8 *ip = module.GetBytecodeForFunction(*func_info);

  // The virtual machine
  VM vm(module);

  // Locals frame
  Frame frame(&vm, func_info->frame_size());

  // Copy args into frame
  std::memcpy(frame.raw_frame() + func_info->params_start_pos(), args,
              func_info->params_size());

  // Let's go
  vm.Interpret(ip, &frame);
}

namespace {

template <typename T>
inline ALWAYS_INLINE T Read(const u8 **ip) {
  static_assert(std::is_integral_v<T>,
                "Read() should only be used to read primitive integer types "
                "directly from the bytecode instruction stream");
  auto ret = *reinterpret_cast<const T *>(*ip);
  (*ip) += sizeof(T);
  return ret;
}

template <typename T>
inline ALWAYS_INLINE T Peek(const u8 **ip) {
  static_assert(std::is_integral_v<T>,
                "Peek() should only be used to read primitive integer types "
                "directly from the bytecode instruction stream");
  return *reinterpret_cast<const T *>(*ip);
}

}  // namespace

// NOLINTNEXTLINE(google-readability-function-size,readability-function-size)
void VM::Interpret(const u8 *ip, Frame *frame) {
  static void *kDispatchTable[] = {
#define ENTRY(name, ...) &&op_##name,
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  };

#ifdef TPL_DEBUG_TRACE_INSTRUCTIONS
#define DEBUG_TRACE_INSTRUCTIONS(op)                                        \
  do {                                                                      \
    auto bytecode = Bytecodes::FromByte(op);                                \
    bytecode_counts_[op]++;                                                 \
    LOG_INFO("{0:p}: {1:s}", ip - sizeof(std::underlying_type_t<Bytecode>), \
             Bytecodes::ToString(bytecode));                                \
  } while (false)
#else
#define DEBUG_TRACE_INSTRUCTIONS(op) (void)op
#endif

  // TODO(pmenon): Should these READ/PEEK macros take in a vm::OperandType so
  // that we can infer primitive types using traits? This minimizes number of
  // changes if the underlying offset/bytecode/register sizes changes?
#define PEEK_JMP_OFFSET() Peek<i32>(&ip)
#define READ_IMM1() Read<i8>(&ip)
#define READ_IMM2() Read<i16>(&ip)
#define READ_IMM4() Read<i32>(&ip)
#define READ_IMM8() Read<i64>(&ip)
#define READ_UIMM2() Read<u16>(&ip)
#define READ_UIMM4() Read<u32>(&ip)
#define READ_JMP_OFFSET() READ_IMM4()
#define READ_LOCAL_ID() Read<u32>(&ip)
#define READ_OP() Read<std::underlying_type_t<Bytecode>>(&ip)

#define OP(name) op_##name
#define DISPATCH_NEXT()           \
  do {                            \
    auto op = READ_OP();          \
    DEBUG_TRACE_INSTRUCTIONS(op); \
    goto *kDispatchTable[op];     \
  } while (false)

  /*****************************************************************************
   *
   * Below this comment begins the primary section of TPL's register-based
   * virtual machine (VM) dispatch area. The VM uses indirect threaded
   * interpretation; each bytecode handler's label is statically generated and
   * stored in @ref kDispatchTable at server compile time. Bytecode handler
   * logic is written as a case using the CASE_OP macro. Handlers can read from
   * and write to registers using the local execution frame's register file
   * (i.e., through @ref Frame::LocalAt()).
   *
   * Upon entry, the instruction pointer (IP) points to the first bytecode of
   * function that is running. The READ_* macros can be used to directly read
   * values from the bytecode stream. The READ_* macros read values from the
   * bytecode stream and advance the IP whereas the PEEK_* macros do only the
   * former, leaving the IP unmodified.
   *
   * IMPORTANT:
   * ----------
   * Bytecode handler code here should only be simple register/IP manipulation
   * (i.e., reading from and writing to registers). Actual full-blown bytecode
   * logic must be implemented externally and invoked from stubs here. This is a
   * strict requirement necessary because it makes code generation to LLVM much
   * simpler.
   *
   ****************************************************************************/

  // Jump to the first instruction
  DISPATCH_NEXT();

  // -------------------------------------------------------
  // Primitive comparison operations
  // -------------------------------------------------------

#define DO_GEN_COMPARISON(op, type)                       \
  OP(op##_##type) : {                                     \
    auto *dest = frame->LocalAt<bool *>(READ_LOCAL_ID()); \
    auto lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    auto rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_COMPARISON_TYPES(type, ...)     \
  DO_GEN_COMPARISON(GreaterThan, type)      \
  DO_GEN_COMPARISON(GreaterThanEqual, type) \
  DO_GEN_COMPARISON(Equal, type)            \
  DO_GEN_COMPARISON(LessThan, type)         \
  DO_GEN_COMPARISON(LessThanEqual, type)    \
  DO_GEN_COMPARISON(NotEqual, type)

  INT_TYPES(GEN_COMPARISON_TYPES)
#undef GEN_COMPARISON_TYPES
#undef DO_GEN_COMPARISON

  // -------------------------------------------------------
  // Primitive arithmetic and binary operations
  // -------------------------------------------------------

#define DO_GEN_ARITHMETIC_OP(op, test, type)              \
  OP(op##_##type) : {                                     \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    type *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    type lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    type rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    if ((test) && rhs == 0u) {                            \
      /* TODO(pmenon): Proper error */                    \
      LOG_ERROR("Division by zero error!");               \
    }                                                     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_ARITHMETIC_OP(type, ...)        \
  DO_GEN_ARITHMETIC_OP(Add, false, type)    \
  DO_GEN_ARITHMETIC_OP(Sub, false, type)    \
  DO_GEN_ARITHMETIC_OP(Mul, false, type)    \
  DO_GEN_ARITHMETIC_OP(Div, true, type)     \
  DO_GEN_ARITHMETIC_OP(Rem, true, type)     \
  DO_GEN_ARITHMETIC_OP(BitAnd, false, type) \
  DO_GEN_ARITHMETIC_OP(BitOr, false, type)  \
  DO_GEN_ARITHMETIC_OP(BitXor, false, type)

  INT_TYPES(GEN_ARITHMETIC_OP)
#undef GEN_ARITHMETIC_OP
#undef DO_GEN_ARITHMETIC_OP

  // -------------------------------------------------------
  // Bitwise and integer negation
  // -------------------------------------------------------

#define GEN_NEG_OP(type, ...)                             \
  OP(Neg##_##type) : {                                    \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpNeg##_##type(dest, input);                          \
    DISPATCH_NEXT();                                      \
  }                                                       \
  OP(BitNeg##_##type) : {                                 \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpBitNeg##_##type(dest, input);                       \
    DISPATCH_NEXT();                                      \
  }

  INT_TYPES(GEN_NEG_OP)
#undef GEN_NEG_OP

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  OP(Jump) : {
    auto skip = PEEK_JMP_OFFSET();
    if (TPL_LIKELY(OpJump())) {
      ip += skip;
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfTrue) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    auto skip = PEEK_JMP_OFFSET();
    if (OpJumpIfTrue(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfFalse) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    auto skip = PEEK_JMP_OFFSET();
    if (OpJumpIfFalse(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Low-level memory operations
  // -------------------------------------------------------

#define GEN_DEREF(type, size)                             \
  OP(Deref##size) : {                                     \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    auto *src = frame->LocalAt<type *>(READ_LOCAL_ID());  \
    OpDeref##size(dest, src);                             \
    DISPATCH_NEXT();                                      \
  }
  GEN_DEREF(i8, 1);
  GEN_DEREF(i16, 2);
  GEN_DEREF(i32, 4);
  GEN_DEREF(i64, 8);
#undef GEN_DEREF

  OP(DerefN) : {
    auto *dest = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto len = READ_UIMM4();
    OpDerefN(dest, src, len);
    DISPATCH_NEXT();
  }

#define GEN_ASSIGN(type, size)                            \
  OP(Assign##size) : {                                    \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    auto src = frame->LocalAt<type>(READ_LOCAL_ID());     \
    OpAssign##size(dest, src);                            \
    DISPATCH_NEXT();                                      \
  }                                                       \
  OP(AssignImm##size) : {                                 \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    OpAssignImm##size(dest, READ_IMM##size());            \
    DISPATCH_NEXT();                                      \
  }
  GEN_ASSIGN(i8, 1);
  GEN_ASSIGN(i16, 2);
  GEN_ASSIGN(i32, 4);
  GEN_ASSIGN(i64, 8);
#undef GEN_ASSIGN

  OP(Lea) : {
    auto **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto offset = READ_UIMM4();
    OpLea(dest, src, offset);
    DISPATCH_NEXT();
  }

  OP(LeaScaled) : {
    auto **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto index = frame->LocalAt<u32>(READ_LOCAL_ID());
    auto scale = READ_UIMM4();
    auto offset = READ_UIMM4();
    OpLeaScaled(dest, src, index, scale, offset);
    DISPATCH_NEXT();
  }

  OP(Call) : {
    ip = ExecuteCall(ip, frame);
    DISPATCH_NEXT();
  }

  OP(Return) : {
    OpReturn();
    return;
  }

  // -------------------------------------------------------
  // Table Vector and Vector Projection Iterator (VPI) ops
  // -------------------------------------------------------

  OP(TableVectorIteratorInit) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    auto table_id = READ_UIMM2();
    OpTableVectorIteratorInit(iter, table_id);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorClose) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorClose(iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorGetVPI) : {
    auto *vpi =
        frame->LocalAt<sql::VectorProjectionIterator **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorGetVPI(vpi, iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // VPI iteration operations
  // -------------------------------------------------------

  OP(VPIHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter =
        frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(VPIAdvance) : {
    auto *iter =
        frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIAdvance(iter);
    DISPATCH_NEXT();
  }

  OP(VPIReset) : {
    auto *iter =
        frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIReset(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // VPI element access
  // -------------------------------------------------------

#define GEN_VPI_ACCESS(type_str, type)                                    \
  OP(VPIGet##type_str) : {                                                \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */                      \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());               \
    auto *vpi =                                                           \
        frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                          \
    OpVPIGet##type_str(result, vpi, col_idx);                             \
    DISPATCH_NEXT();                                                      \
  }                                                                       \
  OP(VPIGet##type_str##Null) : {                                          \
    /* NOLINTNEXTLINE(bugprone-macro-parentheses) */                      \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());               \
    auto *vpi =                                                           \
        frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                          \
    OpVPIGet##type_str##Null(result, vpi, col_idx);                       \
    DISPATCH_NEXT();                                                      \
  }
  GEN_VPI_ACCESS(SmallInt, sql::Integer)
  GEN_VPI_ACCESS(Integer, sql::Integer)
  GEN_VPI_ACCESS(BigInt, sql::Integer)
  GEN_VPI_ACCESS(Decimal, sql::Decimal)
#undef GEN_VPI_ACCESS

#define GEN_VPI_FILTER(Op)                                                \
  OP(VPIFilter##Op) : {                                                   \
    auto *size = frame->LocalAt<u32 *>(READ_LOCAL_ID());                  \
    auto *iter =                                                          \
        frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                          \
    auto val = READ_IMM8();                                               \
    OpVPIFilter##Op(size, iter, col_idx, val);                            \
    DISPATCH_NEXT();                                                      \
  }
  GEN_VPI_FILTER(Equal)
  GEN_VPI_FILTER(GreaterThan)
  GEN_VPI_FILTER(GreaterThanEqual)
  GEN_VPI_FILTER(LessThan)
  GEN_VPI_FILTER(LessThanEqual)
  GEN_VPI_FILTER(NotEqual)
#undef GEN_VPI_FILTER

  // -------------------------------------------------------
  // SQL Integer Comparison Operations
  // -------------------------------------------------------

  OP(ForceBoolTruth) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *sql_int = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    OpForceBoolTruth(result, sql_int);
    DISPATCH_NEXT();
  }

  OP(InitInteger) : {
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<i32>(READ_LOCAL_ID());
    OpInitInteger(sql_int, val);
    DISPATCH_NEXT();
  }

#define GEN_CMP(op)                                                 \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID()); \
    auto *left = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());   \
    auto *right = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());  \
    Op##op##Integer(result, left, right);                           \
    DISPATCH_NEXT();                                                \
  }
  GEN_CMP(GreaterThan);
  GEN_CMP(GreaterThanEqual);
  GEN_CMP(Equal);
  GEN_CMP(LessThan);
  GEN_CMP(LessThanEqual);
  GEN_CMP(NotEqual);
#undef GEN_CMP

  // -------------------------------------------------------
  // Aggregations
  // -------------------------------------------------------

  OP(CountAggregateInit) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpCountAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(CountAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(CountAggregateReset) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateFree) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateInit) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpCountStarAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateReset) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateFree) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerSumAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerSumAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateFree(agg);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Hash Joins
  // -------------------------------------------------------

  OP(JoinHashTableAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *join_hash_table =
        frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpJoinHashTableAllocTuple(result, join_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableBuild) : {
    auto *join_hash_table =
        frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableBuild(join_hash_table);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Sorting
  // -------------------------------------------------------

  OP(SorterInit) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto cmp_func_id = frame->LocalAt<FunctionId>(READ_UIMM2());
    auto tuple_size = READ_UIMM4();

    auto cmp_fn = reinterpret_cast<sql::Sorter::ComparisonFunction>(
        module().GetFuncTrampoline(cmp_func_id));
    OpSorterInit(sorter, nullptr, cmp_fn, tuple_size);
    DISPATCH_NEXT();
  }

  OP(SorterAllocInputTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterAllocInputTuple(result, sorter);
    DISPATCH_NEXT();
  }

  OP(SorterAllocInputTupleTopK) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = READ_IMM8();
    OpSorterAllocInputTupleTopK(result, sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterAllocInputTupleTopKFinish) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = READ_IMM8();
    OpSorterAllocInputTupleTopKFinish(sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterSort) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterSort(sorter);
    DISPATCH_NEXT();
  }

  OP(SorterFree) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterFree(sorter);
    DISPATCH_NEXT();
  }

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");
}

const u8 *VM::ExecuteCall(const u8 *ip, VM::Frame *caller) {
  //
  // Read the function ID and the argument count to the function first
  //

  auto func_id = READ_UIMM2();
  auto num_params = READ_UIMM2();

  //
  // Lookup the function
  //

  const FunctionInfo *func = module().GetFuncInfoById(func_id);
  TPL_ASSERT(func != nullptr, "Function doesn't exist in module!");

  //
  // Create the function's execution frame, and initialize it with the call
  // arguments encoded in the instruction stream
  //

  VM::Frame callee(this, func->frame_size());

  u8 *const raw_frame = callee.raw_frame();
  for (u32 i = 0; i < num_params; i++) {
    const LocalInfo &param_info = func->locals()[i];
    const void *const param = caller->LocalAt<void *>(READ_LOCAL_ID());
    std::memcpy(raw_frame + param_info.offset(), &param, param_info.size());
  }

  //
  // Frame preparation is complete. Let's bounce ...
  //

  const u8 *bytecode = module().GetBytecodeForFunction(*func);
  TPL_ASSERT(bytecode != nullptr, "Bytecode cannot be null");
  Interpret(bytecode, &callee);

  return ip;
}

}  // namespace tpl::vm
