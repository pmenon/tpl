#include "vm/vm.h"

#include "logging/logger.h"
#include "vm/bytecode_function.h"
#include "vm/bytecode_handlers.inline.h"
#include "vm/frame.h"
#include "vm/stack.h"

namespace tpl::vm {

VM::VM() { TPL_MEMSET(bytecode_counts_, 0, sizeof(bytecode_counts_)); }

VmValue VM::Invoke(BytecodeFunction &function) {
  Frame frame(function.max_locals(), function.max_stack());

  frame.set_function(&function);

  VM vm{};
  vm.Run(frame);

  // TODO
  return 0;
}

void VM::Run(Frame &frame) {
  static void *kDispatchTable[] = {
#define OPCODE(name, ...) &&op_##name,
      BYTECODES_LIST(OPCODE)
#undef OPCODE
  };

  const BytecodeFunction *func = frame.function();
  const uint8_t *ip = func->code();

  Stack &stack = frame.stack();
  UNUSED Locals &locals = frame.locals();
  const ConstantsArray &constants = func->constants();

#if TPL_DEBUG_TRACE_INSTRUCTIONS
#define DEBUG_TRACE_INSTRUCTIONS(byte)                                       \
  do {                                                                       \
    Bytecode code = Bytecodes::FromByte(byte);                               \
    bytecode_counts_[static_cast<std::underlying_type_t<Bytecode>>(code)]++; \
    LOG_INFO("{0:p}: {1:s}", ip, Bytecodes::ToString(code));                 \
  } while (false)
#else
#define DEBUG_TRACE_INSTRUCTIONS(byte) (void)byte;
#endif

#define INTERPRETER_LOOP DISPATCH();
#define CASE_OP(name) op_##name
#define DISPATCH()                  \
  do {                              \
    auto byte = (*ip++);            \
    DEBUG_TRACE_INSTRUCTIONS(byte); \
    goto *kDispatchTable[byte];     \
  } while (false)

  INTERPRETER_LOOP {
    /*
     * Integer comparisons and bit-twiddling codes
     */
#define OP_INT(opname, opimpl) \
  CASE_OP(opname) : {          \
    auto v2 = stack.PopInt();  \
    auto v1 = stack.TopInt();  \
    auto ret = opimpl(v1, v2); \
    stack.SetTopInt(ret);      \
    DISPATCH();                \
  }
    OP_INT(Lt, OpIntLt);
    OP_INT(Le, OpIntLe);
    OP_INT(Eq, OpIntEq);
    OP_INT(Gt, OpIntGt);
    OP_INT(Ge, OpIntGe);
    OP_INT(Shl, OpIntShl);
    OP_INT(Shr, OpIntShr);
    OP_INT(Ushr, OpIntUshr);
#undef OP_INT

    /*
     * Binary integer arithmetic codes
     */
#define OP_INT_MATH(opname, opimpl, test)   \
  CASE_OP(opname) : {                       \
    if (test && stack.TopInt() == 0) {      \
      /* TODO(pmenon): Proper error */      \
      LOG_ERROR("Division by zero error!"); \
    }                                       \
    auto v2 = stack.PopInt();               \
    auto v1 = stack.TopInt();               \
    auto ret = opimpl(v1, v2);              \
    stack.SetTopInt(ret);                   \
    DISPATCH();                             \
  }
    OP_INT_MATH(Add, OpIntAdd, false);
    OP_INT_MATH(Sub, OpIntSub, false);
    OP_INT_MATH(Mul, OpIntMul, false);
    OP_INT_MATH(Div, OpIntDiv, true);
    OP_INT_MATH(Rem, OpIntRem, true);
#undef OP_INT_MATH

#define OP_BRANCH(opname, op)                                  \
  CASE_OP(opname) : {                                          \
    auto v2 = stack.PopInt();                                  \
    auto v1 = stack.PopInt();                                  \
    if (v1 op v2) {                                            \
      auto skip = (static_cast<uint16_t>(ip[0]) << 8) | ip[1]; \
      ip += skip;                                              \
    }                                                          \
    DISPATCH();                                                \
  }
    OP_BRANCH(Beq, ==)
    OP_BRANCH(Bge, >=)
    OP_BRANCH(Bgt, >)
    OP_BRANCH(Ble, <=)
    OP_BRANCH(Blt, <)
    OP_BRANCH(Bne, !=)
#undef OP_BRANCHES

    CASE_OP(Br) : {
      auto offset = (static_cast<uint16_t>(ip[0]) << 8) | ip[1];
      ip += offset;
      DISPATCH();
    }

    /*
     * Integer negation
     */
    CASE_OP(Neg) : {
      stack.SetTopInt(OpIntNeg(stack.TopInt()));
      DISPATCH();
    }

    /*
     * Constant loading
     */
    CASE_OP(Ldc) : {
      uint16_t idx = (static_cast<uint16_t>(ip[0]) << 8) | ip[1];
      ip += 2;
      switch (constants.TypeAt(idx)) {
        case VmObjType::Int: {
          stack.PushInt(constants.ObjAt<VmInt>(idx));
          break;
        }
        case VmObjType::Float: {
          stack.PushFloat(constants.ObjAt<VmFloat>(idx));
          break;
        }
        case VmObjType::Reference: {
          stack.PushReference(constants.ObjAt<VmReference>(idx));
          break;
        }
      }
      DISPATCH();
    }

    /*
     * Return
     */
    CASE_OP(Ret) : { return; }

    // End interpreter loop
  }

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");

#undef DISPATCH
#undef CASE_OP
#undef INTERPRETER_LOOP
#undef DEBUG_TRACE_INSTRUCTIONS
}  // namespace tpl::vm

}  // namespace tpl::vm