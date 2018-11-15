#include "vm/vm.h"

#include "logging/logger.h"
#include "sql/table.h"
#include "sql/value.h"
#include "util/common.h"
#include "util/timer.h"
#include "vm/bytecode_handlers.h"
#include "vm/function_info.h"
#include "vm/module.h"

namespace tpl::vm {

/**
 * An execution frame where all function's local variables and parameters live
 * for the duration of the function's lifetime.
 */
class VM::Frame {
 public:
  Frame(VM *vm, std::size_t frame_size) : vm_(vm), frame_size_(frame_size) {
    frame_data_ = vm->AllocateFrame(frame_size);
    TPL_ASSERT(frame_data_ != nullptr, "Frame data cannot be null");
    TPL_ASSERT(frame_size_ >= 0, "Frame size must be >= 0");
  }

  ~Frame() { vm()->ReleaseFrame(frame_size()); }

  template <typename T>
  T LocalAt(u32 index) const {
    LocalVar local_var = LocalVar::Decode(index);

    EnsureInFrame(local_var);

    auto local =
        reinterpret_cast<uintptr_t>(&frame_data_[local_var.GetOffset()]);

    if (local_var.GetAddressMode() == LocalVar::AddressMode::Value) {
      return *(T *)(local);
    }

    return (T)local;
  }

 private:
#ifndef NDEBUG
  void EnsureInFrame(LocalVar var) const {
    if (var.GetOffset() >= frame_size()) {
      std::string error_msg =
          fmt::format("Accessing local at offset {}, beyond frame of size {}",
                      var.GetOffset(), frame_size());
      LOG_ERROR("{}", error_msg);
      throw std::runtime_error(error_msg);
    }
  }
#else
  void EnsureInFrame(UNUSED LocalVar var) const {}
#endif

  VM *vm() const { return vm_; }

  const u8 *raw_frame() const { return frame_data_; }

  std::size_t frame_size() const { return frame_size_; }

 public:
  VM *vm_;
  u8 *frame_data_;
  std::size_t frame_size_;
};

VM::VM(util::Region *region, const Module &module)
    : stack_(kDefaultInitialStackSize, 0, region), sp_(0), module_(module) {
  TPL_MEMSET(bytecode_counts_, 0, sizeof(bytecode_counts_));
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

  // TODO(pmenon): Should these READ/PEEK macros take in a vm::OperantType so
  // that we can infer primitive types using traits? This minimizes number of
  // changes if the underlying offset/bytecode/register sizes changes?
#define PEEK_JMP_OFFSET() Peek<u16>(&ip)
#define READ_IMM1() Read<i8>(&ip)
#define READ_IMM2() Read<i16>(&ip)
#define READ_IMM4() Read<i32>(&ip)
#define READ_IMM8() Read<i64>(&ip)
#define READ_UIMM2() Read<u16>(&ip)
#define READ_UIMM4() Read<u32>(&ip)
#define READ_JMP_OFFSET() READ_UIMM2()
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

  std::vector<void *> params;

  DISPATCH_NEXT();

  /*
   * All primitive comparisons are generated here. Primitive comparisons all
   * have the same bytecode format. The target of the comparison is a primitive
   * single-byte boolean value, and the two operands are the primitive input
   * arguments.
   */
#define DO_GEN_COMPARISON(op, type)                       \
  OP(op##_##type) : {                                     \
    bool *dest = frame->LocalAt<bool *>(READ_LOCAL_ID()); \
    type lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    type rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_COMPARISON_TYPES(type)          \
  DO_GEN_COMPARISON(GreaterThan, type)      \
  DO_GEN_COMPARISON(GreaterThanEqual, type) \
  DO_GEN_COMPARISON(Equal, type)            \
  DO_GEN_COMPARISON(LessThan, type)         \
  DO_GEN_COMPARISON(LessThanEqual, type)    \
  DO_GEN_COMPARISON(NotEqual, type)

  INT_TYPES(GEN_COMPARISON_TYPES)
#undef GEN_COMPARISON_TYPES
#undef DO_GEN_COMPARISON

  /*
   * Binary operations
   */
#define DO_GEN_ARITHMETIC_OP(op, test, type)              \
  OP(op##_##type) : {                                     \
    type *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    type lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    type rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    if (test && rhs == 0u) {                              \
      /* TODO(pmenon): Proper error */                    \
      LOG_ERROR("Division by zero error!");               \
    }                                                     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_ARITHMETIC_OP(type)             \
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

  /*
   * Bitwise negation and regular integer negation
   */
#define GEN_NEG_OP(type)                                  \
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

  /*
   * Jumps are unconditional forward-only jumps
   */
  OP(Jump) : {
    u16 skip = PEEK_JMP_OFFSET();
    if (OpJump()) {
      ip += skip;
    }
    DISPATCH_NEXT();
  }

  /*
   * JumpLoops are unconditional backwards-only jumps, mostly used for loops
   */
  OP(JumpLoop) : {
    u16 skip = PEEK_JMP_OFFSET();
    if (OpJump()) {
      ip -= skip;
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfTrue) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    u16 skip = PEEK_JMP_OFFSET();
    if (OpJumpIfTrue(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfFalse) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    u16 skip = PEEK_JMP_OFFSET();
    if (OpJumpIfFalse(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

#define GEN_LOAD_IMM(type, size)                          \
  OP(LoadImm##size) : {                                   \
    type *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    type val = READ_IMM##size();                          \
    OpLoadImm_##type(dest, val);                          \
    DISPATCH_NEXT();                                      \
  }

  GEN_LOAD_IMM(i8, 1);
  GEN_LOAD_IMM(i16, 2);
  GEN_LOAD_IMM(i32, 4);
  GEN_LOAD_IMM(i64, 8);
#undef GEN_LOAD_IMM

#define GEN_DEREF(type, size)                             \
  OP(Deref##size) : {                                     \
    type *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    type *src = frame->LocalAt<type *>(READ_LOCAL_ID());  \
    OpDeref##size(dest, src);                             \
    DISPATCH_NEXT();                                      \
  }
  GEN_DEREF(i8, 1);
  GEN_DEREF(i16, 2);
  GEN_DEREF(i32, 4);
  GEN_DEREF(i64, 8);
#undef GEN_DEREF

  OP(DerefN) : {
    byte *dest = frame->LocalAt<byte *>(READ_LOCAL_ID());
    byte *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    u32 len = READ_UIMM4();
    OpDerefN(dest, src, len);
    DISPATCH_NEXT();
  }

  OP(Lea) : {
    byte **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    byte *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    u32 offset = READ_UIMM4();
    OpLea(dest, src, offset);
    DISPATCH_NEXT();
  }

  OP(LeaScaled) : {
    byte **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    byte *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    u32 index = frame->LocalAt<u32>(READ_LOCAL_ID());
    u32 scale = READ_UIMM4();
    u32 offset = READ_UIMM4();
    OpLeaScaled(dest, src, index, scale, offset);
    DISPATCH_NEXT();
  }

  OP(Call) : {
    u16 func_id = READ_UIMM2();
    u16 num_params = READ_UIMM2();
    params.reserve(num_params);
    for (u32 i = 0; i < num_params; i++) {
      params[i] = frame->LocalAt<void *>(READ_LOCAL_ID());
    }
    Invoke(func_id);
    DISPATCH_NEXT();
  }

  OP(Return) : { return; }

  OP(SqlTableIteratorInit) : {
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    auto table_id = READ_UIMM2();
    OpSqlTableIteratorInit(iter, table_id);
    DISPATCH_NEXT();
  }

  OP(SqlTableIteratorNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    OpSqlTableIteratorNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(SqlTableIteratorClose) : {
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    OpSqlTableIteratorClose(iter);
    DISPATCH_NEXT();
  }

  OP(ReadSmallInt) : {
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM4();
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpReadSmallInt(iter, col_idx, sql_int);
    DISPATCH_NEXT();
  }

  OP(ReadInteger) : {
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM4();
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpReadInt(iter, col_idx, sql_int);
    DISPATCH_NEXT();
  }

  OP(ReadBigInt) : {
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM4();
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpReadBigInt(iter, col_idx, sql_int);
    DISPATCH_NEXT();
  }

  OP(ReadDecimal) : {
    auto *iter = frame->LocalAt<sql::TableIterator *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM4();
    auto *sql_int = frame->LocalAt<sql::Decimal *>(READ_LOCAL_ID());
    OpReadDecimal(iter, col_idx, sql_int);
    DISPATCH_NEXT();
  }

  OP(ForceBoolTruth) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpForceBoolTruth(result, sql_int);
    DISPATCH_NEXT();
  }

  OP(InitInteger) : {
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    i32 val = frame->LocalAt<i32>(READ_LOCAL_ID());
    OpInitInteger(sql_int, val);
    DISPATCH_NEXT();
  }

#define GEN_CMP(op)                                                 \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID()); \
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

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");
}

void VM::Invoke(u32 func_id) {
  const FunctionInfo *func = module().GetFuncInfoById(func_id);

  if (TPL_UNLIKELY(func == nullptr)) {
    std::string error_msg =
        fmt::format("Function with ID {} does not exist in module", func_id);
    LOG_ERROR("{}", error_msg);
    throw std::runtime_error(error_msg);
  }

  /*
   * We need to set up the frame. First, we allocate enough space for all local
   * variables. Then we setup call arguments.
   */

  std::size_t frame_size = func->frame_size();
  VM::Frame frame(this, frame_size);

  /*
   * The frame has been set up. We are now ready to interpret the function.
   */

  const u8 *bytecode = module().GetBytecodeForFunction(*func);
  TPL_ASSERT(bytecode != nullptr, "Bytecode cannot be null");
  Interpret(bytecode, &frame);

  /*
   * Function has completed. Setup return vals.
   */
}

// static
void VM::Execute(util::Region *region, const Module &module,
                 const std::string &name) {
  auto *func = module.GetFuncInfoByName(name);
  if (func == nullptr) return;

  VM vm(region, module);
  vm.Invoke(func->id());
}

}  // namespace tpl::vm