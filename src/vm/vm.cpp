#include "vm/vm.h"

#include "logging/logger.h"
#include "util/common.h"
#include "util/timer.h"
#include "vm/bytecode_handlers.h"

namespace tpl::vm {

class VM::Frame {
 public:
  Frame(VM *vm, const BytecodeUnit &unit, const FunctionInfo &func)
      : vm_(vm),
        caller_(vm->current_frame()),
        data_(static_cast<u8 *>(std::aligned_alloc(64, func.frame_size())),
              &std::free),
        pc_(unit.GetBytecodeForFunction(func)) {
    TPL_MEMSET(data_.get(), 0, func.frame_size());
    vm->set_current_frame(this);
  }

  ~Frame() { vm()->set_current_frame(caller()); }

  VM *vm() { return vm_; }

  Frame *caller() const { return caller_; }

  const u8 *pc() const { return pc_; }

  template <typename T>
  T LocalAt(u32 index) const {
    LocalVar local_var = LocalVar::Decode(index);

    auto local = reinterpret_cast<uintptr_t>(&data_[local_var.GetOffset()]);

    if (local_var.GetAddressMode() == LocalVar::AddressMode::Value) {
      return *reinterpret_cast<T *>(local);
    }

    return (T)local;
  }

 public:
  VM *vm_;
  Frame *caller_;
  std::unique_ptr<u8[], decltype(&std::free)> data_;
  const u8 *pc_;
};

VM::VM(const BytecodeUnit &unit) : unit_(unit) {
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

void VM::Run(Frame *frame) {
  static void *kDispatchTable[] = {
#define ENTRY(name, ...) &&op_##name,
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  };

#ifdef TPL_DEBUG_TRACE_INSTRUCTIONS
#define DEBUG_TRACE_INSTRUCTIONS()                                         \
  do {                                                                     \
    bytecode_counts_[static_cast<std::underlying_type_t<Bytecode>>(op)]++; \
    LOG_INFO("{0:p}: {1:s}", ip - 1, Bytecodes::ToString(op));             \
  } while (false)
#else
#define DEBUG_TRACE_INSTRUCTIONS()
#endif

  // TODO(pmenon): Should these READ/PEEK macros take in a vm::OperantType so
  // that we can infer primitive types using traits? This minimizes number of
  // changes if the underlying offset/bytecode/register sizes changes?
#define PEEK_JMP_OFFSET() Peek<u16>(&ip)
#define READ_IMM1() Read<i8>(&ip)
#define READ_IMM2() Read<i16>(&ip)
#define READ_IMM4() Read<i32>(&ip)
#define READ_IMM8() Read<i64>(&ip)
#define READ_UIMM4() Read<u32>(&ip);
#define READ_JMP_OFFSET() Read<u16>(&ip)
#define READ_REG_ID() Read<u32>(&ip)
#define READ_REG_COUNT() Read<u16>(&ip)
#define READ_OP() Read<std::underlying_type_t<Bytecode>>(&ip)

#define OP(name) op_##name
#define DISPATCH_NEXT()                          \
  do {                                           \
    op = Bytecodes::FromByte(READ_OP());         \
    DEBUG_TRACE_INSTRUCTIONS();                  \
    goto *kDispatchTable[Bytecodes::ToByte(op)]; \
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

  // The currently executing bytecode and the instruction pointer
  Bytecode op;
  const u8 *ip = frame->pc();

  DISPATCH_NEXT();

  /*
   * All primitive comparisons are generated here. Primitive comparisons all
   * have the same bytecode format. The target of the comparison is a primitive
   * single-byte boolean value, and the two operands are the primitive input
   * arguments.
   */
#define DO_GEN_COMPARISON(op, type)                     \
  OP(op##_##type) : {                                   \
    bool *dest = frame->LocalAt<bool *>(READ_REG_ID()); \
    type lhs = frame->LocalAt<type>(READ_REG_ID());     \
    type rhs = frame->LocalAt<type>(READ_REG_ID());     \
    Op##op##_##type(dest, lhs, rhs);                    \
    DISPATCH_NEXT();                                    \
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
#define DO_GEN_ARITHMETIC_OP(op, test, type)            \
  OP(op##_##type) : {                                   \
    type *dest = frame->LocalAt<type *>(READ_REG_ID()); \
    type lhs = frame->LocalAt<type>(READ_REG_ID());     \
    type rhs = frame->LocalAt<type>(READ_REG_ID());     \
    if (test && rhs == 0u) {                            \
      /* TODO(pmenon): Proper error */                  \
      LOG_ERROR("Division by zero error!");             \
    }                                                   \
    Op##op##_##type(dest, lhs, rhs);                    \
    DISPATCH_NEXT();                                    \
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
#define GEN_NEG_OP(type)                                \
  OP(Neg##_##type) : {                                  \
    auto *dest = frame->LocalAt<type *>(READ_REG_ID()); \
    auto input = frame->LocalAt<type>(READ_REG_ID());   \
    OpNeg##_##type(dest, input);                        \
    DISPATCH_NEXT();                                    \
  }                                                     \
  OP(BitNeg##_##type) : {                               \
    auto *dest = frame->LocalAt<type *>(READ_REG_ID()); \
    auto input = frame->LocalAt<type>(READ_REG_ID());   \
    OpBitNeg##_##type(dest, input);                     \
    DISPATCH_NEXT();                                    \
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
    auto cond = frame->LocalAt<bool>(READ_REG_ID());
    u16 skip = PEEK_JMP_OFFSET();
    if (OpJumpIfTrue(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfFalse) : {
    auto cond = frame->LocalAt<bool>(READ_REG_ID());
    u16 skip = PEEK_JMP_OFFSET();
    if (OpJumpIfFalse(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  OP(LoadImm1) : {
    i8 *dest = frame->LocalAt<i8 *>(READ_REG_ID());
    i8 val = READ_IMM1();
    OpLoadImm_i8(dest, val);
    DISPATCH_NEXT();
  }

  OP(LoadImm2) : {
    i16 *dest = frame->LocalAt<i16 *>(READ_REG_ID());
    i16 val = READ_IMM2();
    OpLoadImm_i16(dest, val);
    DISPATCH_NEXT();
  }

  OP(LoadImm4) : {
    i32 *dest = frame->LocalAt<i32 *>(READ_REG_ID());
    i32 val = READ_IMM4();
    OpLoadImm_i32(dest, val);
    DISPATCH_NEXT();
  }

  OP(LoadImm8) : {
    i64 *dest = frame->LocalAt<i64 *>(READ_REG_ID());
    i64 val = READ_IMM8();
    OpLoadImm_i64(dest, val);
    DISPATCH_NEXT();
  }

  OP(Deref4) : {
    u32 *dest = frame->LocalAt<u32 *>(READ_REG_ID());
    u32 *src = frame->LocalAt<u32 *>(READ_REG_ID());
    OpDeref4(dest, src);
    DISPATCH_NEXT();
  }

  OP(Lea) : {
    byte **dest = frame->LocalAt<byte **>(READ_REG_ID());
    byte *src = frame->LocalAt<byte *>(READ_REG_ID());
    u32 offset = READ_UIMM4();
    OpLea(dest, src, offset);
    DISPATCH_NEXT();
  }

  OP(Return) : {
    // Just return for now. We need to handle return values though ...
    return;
  }

  OP(ScanOpen) : { DISPATCH_NEXT(); }
  OP(ScanNext) : { DISPATCH_NEXT(); }
  OP(ScanClose) : { DISPATCH_NEXT(); }

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");
}

void VM::Invoke(FunctionId func_id) {
  const auto *func = unit().GetFunctionById(func_id);

  if (func == nullptr) return;

  VM::Frame frame(this, unit(), *func);
  Run(&frame);
}

// static
void VM::Execute(const BytecodeUnit &unit, const std::string &name) {
  auto *func = unit.GetFunctionByName(name);
  if (func == nullptr) return;

  VM vm(unit);
  vm.Invoke(func->id());
}

}  // namespace tpl::vm