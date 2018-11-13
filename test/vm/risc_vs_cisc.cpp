#include "gtest/gtest.h"

#include "tpl_test.h"

#include "util/common.h"
#include "util/macros.h"
#include "util/timer.h"

namespace tpl::vm::test {

/**
 * struct S { a: int, b: int, c:int }
 * fun f(p: *S) -> void {
 * var q: S
 * q.a = 1
 * q.b = 2
 * p.a = 3
 * p.b = 4
 * q.c = p.a + p.b + q.a + q.b
 * p.c = q.c + p.b
 * }
 *
 * Loops kNumRuns times
 */

class RiscVsCiscTest : public TplTest {};

struct S {
  u64 a;
  u64 b;
  u64 c;
};

static constexpr u32 kNumRuns = 10000000;

class StackVM {
 public:
  enum class Op : u8 { Local, Add, Load, Store, Const, Jump };

  static void Interp(u64 *locals, u8 *ops) {
    static const void *dispatch[] = {&&op_Local, &&op_Add,   &&op_Load,
                                     &&op_Store, &&op_Const, &&op_Jump};

#define READ_BYTE() (*ip++)
#define READ_REG() READ_BYTE()
#define NEXT() goto *dispatch[READ_BYTE()];

    u64 stack[100];
    u32 stack_offset = 0;

    u32 i = 0;
    u8 *ip = ops;

    NEXT();

  op_Local : {
    stack[stack_offset] = (u64)&locals[READ_BYTE()];
    stack_offset++;
    NEXT();
  };

  op_Add : {
    u64 r0 = stack[stack_offset - 2];
    u64 r1 = stack[stack_offset - 1];
    stack[stack_offset - 2] = r0 + r1;
    stack_offset--;
    NEXT();
  };

  op_Load : {
    u64 *r0 = (u64 *)stack[stack_offset - 1];
    stack[stack_offset - 1] = *r0;
    NEXT();
  };

  op_Store : {
    u64 *dest = (u64 *)stack[stack_offset - 2];
    u64 val = stack[stack_offset - 1];
    *dest = val;
    stack_offset -= 2;
    NEXT();
  };

  op_Const : {
    stack[stack_offset] = READ_BYTE();
    stack_offset++;
    NEXT();
  };

  op_Jump : {
    if (i++ > kNumRuns) {
      return;
    }
    ip = ops;
    NEXT();
  };
  }
};

class Risc {
 public:
  using OpSize = u8;

  enum class Op : OpSize {
    LoadAddr,
    LoadInd,
    Assign,
    StoreImm,
    Store,
    Add,
    AddImm,
    Jump,
  };

  static void Interp(u64 *frame, OpSize *ops) {
    static const void *dispatch[] = {
        &&op_LoadAddr, &&op_LoadInd, &&op_Assign, &&op_StoreImm,
        &&op_Store,    &&op_Add,     &&op_AddImm, &&op_Jump,
    };

#define READ_BYTE() (*ip++)
#define READ_REG() READ_BYTE()
#define NEXT() goto *dispatch[READ_BYTE()];

    u32 i = 0;
    OpSize *ip = ops;

    NEXT();

  op_LoadAddr : {
    // std::cout << "LoadAddr" << std::endl;
    auto dest = READ_REG();
    auto offset = READ_REG();
    auto src = READ_REG();
    frame[dest] = (u64)(&frame[src] + offset / 8);
    NEXT();
  };

  op_LoadInd : {
    // std::cout << "LoadInd" << std::endl;
    auto dest = READ_REG();
    auto offset = READ_REG();
    auto src = READ_REG();
    frame[dest] = *((u64 *)frame[src] + offset / 8);
    NEXT();
  };

  op_Assign : {
    auto dest = READ_REG();
    auto src = READ_REG();
    frame[dest] = frame[src];
    NEXT();
  };

  op_Store : {
    // std::cout << "Store" << std::endl;
    auto src = READ_REG();
    auto offset = READ_REG();
    auto dest = READ_REG();
    *((u64 *)frame[dest] + offset / 8) = frame[src];
    NEXT();
  };

  op_StoreImm : {
    // std::cout << "StoreImm" << std::endl;
    auto imm = READ_REG();
    auto offset = READ_REG();
    auto dest = READ_REG();
    *((u64 *)frame[dest] + offset / 8) = imm;
    NEXT();
  };

  op_Add : {
    // std::cout << "Add" << std::endl;
    auto dest = READ_REG();
    auto src1 = READ_REG();
    auto src2 = READ_REG();
    frame[dest] = frame[src1] + frame[src2];
    NEXT();
  };

  op_AddImm : {
    // std::cout << "AddImm" << std::endl;
    auto dest = READ_REG();
    auto src1 = READ_REG();
    auto num = READ_REG();
    frame[dest] = frame[src1] + num;
    NEXT();
  };

  op_Jump : {
    // std::cout << "Jump" << std::endl;
    if (i++ > kNumRuns) {
      return;
    }
    ip = ops;
    NEXT();
  }

#undef NEXT
#undef READ_REG
#undef READ_BYTE
  }
};

class Risc2 {
 public:
  enum class Op : u8 {
    LoadAddr,
    Load,
    Lea,
    StoreImm,
    Store,
    Add,
    AddImm,
    Jump,
  };

  static void Interp(u64 *frame, u8 *ops) {
    static const void *dispatch[] = {
        &&op_LoadAddr, &&op_Load, &&op_Lea,    &&op_StoreImm,
        &&op_Store,    &&op_Add,  &&op_AddImm, &&op_Jump,
    };

#define READ_BYTE() (*ip++)
#define READ_REG() READ_BYTE()
#define NEXT() goto *dispatch[READ_BYTE()];

    u32 i = 0;
    u8 *ip = ops;

    NEXT();

  op_LoadAddr : {
    auto dest = READ_REG();
    auto src = READ_REG();
    frame[dest] = (u64)&frame[src];
    NEXT();
  };

  op_Load : {
    auto dest = READ_REG();
    auto src = READ_REG();
    frame[dest] = (u64)(*(u64 **)frame[src]);
    NEXT();
  };

  op_Lea : {
    auto dest = READ_REG();
    auto src = READ_REG();
    auto offset = READ_REG();
    frame[dest] = (u64)((u64 *)frame[src] + offset / 8);
    NEXT();
  };

  op_Store : {
    auto dest = READ_REG();
    auto src = READ_REG();
    *((u64 *)frame[dest]) = frame[src];
    NEXT();
  };

  op_StoreImm : {
    auto dest = READ_REG();
    auto imm = READ_REG();
    *(u64 *)frame[dest] = imm;
    NEXT();
  };

  op_Add : {
    auto dest = READ_REG();
    auto src1 = READ_REG();
    auto src2 = READ_REG();
    frame[dest] = frame[src1] + frame[src2];
    NEXT();
  };

  op_AddImm : {
    auto dest = READ_REG();
    auto src1 = READ_REG();
    auto num = READ_REG();
    frame[dest] = frame[src1] + num;
    NEXT();
  };

  op_Jump : {
    if (i++ > kNumRuns) {
      return;
    }
    ip = ops;
    NEXT();
  }

#undef NEXT
#undef READ_REG
#undef READ_BYTE
  }
};

class Cisc {
 public:
  enum class Op : u8 {
    Lea,
    LoadImm,
    Add,
    Jump,
  };

  template <bool Indirect>
  static u8 AsReg(u8 reg) {
    return (reg << 1) | (u8)Indirect;
  }

  template <typename T>
  static ALWAYS_INLINE T *Read(u64 *frame, u8 reg_id) {
    u8 r = reg_id >> 1;
    bool indirect = (reg_id & 0x1) == 1;
    auto v = reinterpret_cast<T *>(&frame[r]);
    if (indirect) {
      v = *reinterpret_cast<T **>(v);
    }
    return v;
  }

  static void Interp(u64 *frame, u8 *ops) {
    static const void *dispatch[] = {&&op_Lea, &&op_LoadImm, &&op_Add,
                                     &&op_Jump};

#define READ_BYTE() (*ip++)
#define READ_REG() READ_BYTE()
#define NEXT() goto *dispatch[READ_BYTE()];

    u32 i = 0;
    u8 *ip = ops;

    NEXT();

  op_Lea : {
    auto *dest = Read<u8 *>(frame, READ_REG());
    auto *base = Read<u8>(frame, READ_REG());
    auto offset = READ_BYTE();
    *dest = base + offset;
    NEXT();
  }

  op_LoadImm : {
    auto *dest = Read<u64>(frame, READ_REG());
    auto num = READ_BYTE();
    *dest = num;
    NEXT();
  }

  op_Add : {
    auto *dest = Read<u64>(frame, READ_REG());
    auto *src1 = Read<u64>(frame, READ_REG());
    auto *src2 = Read<u64>(frame, READ_REG());
    *dest = *src1 + *src2;
    NEXT();
  }

  op_Jump : {
    if (i++ > kNumRuns) {
      // std::cout << ((S *)&frame[1])->a << std::endl;
      return;
    }
    ip = ops;
    NEXT();
  }
  }
};

TEST_F(RiscVsCiscTest, StackVM) {
  S param{.a = 0, .b = 1, .c = 0};

  u64 locals[] = {
      (u64)&param,  // R0: param *S
      0,            // R1: local q S
      0,            // R2: local q S
      0,            // R3: local q S
  };

  // clang-format off
  u8 ops[] = {
      static_cast<u8>(StackVM::Op::Local), 1,                // s = [&q]
//      static_cast<u8>(StackVM::Op::Const), 0,                // s = [&q, 0]
//      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.a]
      static_cast<u8>(StackVM::Op::Const), 1,                // s = [&q.a, 1]
      static_cast<u8>(StackVM::Op::Store),                   // s = [], q.a = 1

      static_cast<u8>(StackVM::Op::Local), 1,                // s = [&q]
      static_cast<u8>(StackVM::Op::Const), 8,                // s = [&q, 8]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.b]
      static_cast<u8>(StackVM::Op::Const), 2,                // s = [&q.b, 2]
      static_cast<u8>(StackVM::Op::Store),                   // s = [], q.b = 2

      static_cast<u8>(StackVM::Op::Local), 0,                // s = [&*p]
      static_cast<u8>(StackVM::Op::Load),                    // s = [*p]
//      static_cast<u8>(StackVM::Op::Const), 0,                // s = [*p, 0]
//      static_cast<u8>(StackVM::Op::Add),                     // s = [&p.a]
      static_cast<u8>(StackVM::Op::Const), 3,                // s = [&p.a, 3]
      static_cast<u8>(StackVM::Op::Store),                   // s = [], p.a = 3

      static_cast<u8>(StackVM::Op::Local), 0,                // s = [&*p]
      static_cast<u8>(StackVM::Op::Load),                    // s = [*p]
      static_cast<u8>(StackVM::Op::Const), 8,                // s = [*p, 8]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&p.b]
      static_cast<u8>(StackVM::Op::Const), 4,                // s = [&p.b, 4]
      static_cast<u8>(StackVM::Op::Store),                   // s = [], p.b = 4

      static_cast<u8>(StackVM::Op::Local), 1,                // s = [&q]
      static_cast<u8>(StackVM::Op::Const), 16,               // s = [&q, 16]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c]
      static_cast<u8>(StackVM::Op::Local), 0,                // s = [&q.c, &*p]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&q.c, *p]
//      static_cast<u8>(StackVM::Op::Const), 0,                // s = [&q.c, *p, 0]
//      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, &p.a]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&q.c, (p.a)]
      static_cast<u8>(StackVM::Op::Local), 0,                // s = [&q.c, (p.a), &*p]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&q.c, (p.a), *p]
      static_cast<u8>(StackVM::Op::Const), 8,                // s = [&q.c, (p.a), *p, 8]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, (p.a), &p.b]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&q.c, (p.a), (p.b)]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, (p.a + p.b)]

      static_cast<u8>(StackVM::Op::Local), 1,                // s = [&q.c, (p.a + p.b), &q]
      static_cast<u8>(StackVM::Op::Const), 0,                // s = [&q.c, (p.a + p.b), &q, 0]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, (p.a + p.b), &q.a]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&q.c, (p.a + p.b), (q.a)]
      static_cast<u8>(StackVM::Op::Local), 1,                // s = [&q.c, (p.a + p.b), (q.a), &q]
      static_cast<u8>(StackVM::Op::Const), 8,                // s = [&q.c, (p.a + p.b), (q.a), &q, 8]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, (p.a + p.b), (q.a), &q.b]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&q.c, (p.a + p.b), (q.a), (q.b)]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, (p.a + p.b), (q.a + q.b)]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&q.c, (p.a + p.b + q.a + q.b)]
      static_cast<u8>(StackVM::Op::Store),                   // s = [], q.c = p.a + p.b + q.a + q.b

      static_cast<u8>(StackVM::Op::Local), 0,                // s = [&*p]
      static_cast<u8>(StackVM::Op::Load),                    // s = [*p]
      static_cast<u8>(StackVM::Op::Const), 16,               // s = [*p, 16]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&p.c]
      static_cast<u8>(StackVM::Op::Local), 1,                // s = [&p.c, &q]
      static_cast<u8>(StackVM::Op::Const), 16,               // s = [&p.c, &q, 16]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&p.c, &q.c]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&p.c, (q.c)]
      static_cast<u8>(StackVM::Op::Local), 0,                // s = [&p.c, (q.c), &*p]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&p.c, (q.c), *p]
      static_cast<u8>(StackVM::Op::Const), 8,                // s = [&p.c, (q.c), *p, 8]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&p.c, (q.c), &p.b]
      static_cast<u8>(StackVM::Op::Load),                    // s = [&p.c, (q.c), (p.b)]
      static_cast<u8>(StackVM::Op::Add),                     // s = [&p.c, (q.c + p.b)]
      static_cast<u8>(StackVM::Op::Store),                   // s = [], p.c = q.c + p.b

      static_cast<u8>(StackVM::Op::Jump)
  };
  // clang-format on

  util::Timer<std::milli> watch;

  watch.Start();

  StackVM::Interp(locals, ops);

  watch.Stop();
  std::cout << "q.a: " << ((S *)&locals[1])->a << std::endl;
  std::cout << "q.b: " << ((S *)&locals[1])->b << std::endl;
  std::cout << "q.c: " << ((S *)&locals[1])->c << std::endl;
  std::cout << "p.a: " << param.a << std::endl;
  std::cout << "p.b: " << param.b << std::endl;
  std::cout << "p.c: " << param.c << std::endl;
  std::cout << "Duration: " << watch.elapsed() << " ms" << std::endl;
}

TEST_F(RiscVsCiscTest, Risc) {
  S param{.a = 0, .b = 1, .c = 0};

  u64 frame[] = {
      (u64)&param,  // R0: param *S
      0,            // R1: local q S
      0,            // R2: local q S
      0,            // R3: local q S
      0,            // R4: T0
      0,            // R5: T1
      0,            // R6: T2
      0,            // R7: T3
      0,            // R8: T4
      0,            // R9: T5
      0,            // R10: T6
      0,            // R11: T7
      0,            // R12: T8
      0,            // R13: T9
      0,            // R14: T10
      0,            // R15: T11
      0,            // R16: T12
      0,            // R17: T13
      0,            // R18: T14
      0,            // R19: T15
      0,            // R20: A0
      0,            // R21: A1
      0,            // R22: A2
      0,            // R23: A3
      0,            // R24: A4
      0,            // R25: A5
  };

  // clang-format off
  Risc::OpSize ops[] = {
      static_cast<Risc::OpSize>(Risc::Op::LoadAddr), 20, 0, 1,  // lea A0, 0(R1)
      static_cast<Risc::OpSize>(Risc::Op::StoreImm), 1, 0, 20,  // si 1, 0(A0)
      static_cast<Risc::OpSize>(Risc::Op::LoadAddr), 21, 0, 1,  // lea A1, 0(R1)
      static_cast<Risc::OpSize>(Risc::Op::StoreImm), 2, 8, 21,  // si 2, 8(A1)
      static_cast<Risc::OpSize>(Risc::Op::StoreImm), 3, 0, 0,   // si 3, 0(R0)
      static_cast<Risc::OpSize>(Risc::Op::StoreImm), 4, 8, 0,   // si 4, 8(R0)
      static_cast<Risc::OpSize>(Risc::Op::LoadInd), 7, 0, 0,    // lw T3, 0(R0)
      static_cast<Risc::OpSize>(Risc::Op::LoadInd), 9, 8, 0,    // lw T5, 8(R0)
      static_cast<Risc::OpSize>(Risc::Op::Add), 10, 7, 9,       // add T6, T3, T5
      static_cast<Risc::OpSize>(Risc::Op::LoadAddr), 22, 0, 1,  // lea A2, 0(R1)
      static_cast<Risc::OpSize>(Risc::Op::LoadInd), 11, 0, 22,  // lw T7, 0(A2)
      static_cast<Risc::OpSize>(Risc::Op::Add), 12, 10, 11,     // add T8, T6, T7
      static_cast<Risc::OpSize>(Risc::Op::LoadAddr), 23, 0, 1,  // lea A3, 0(R1)
      static_cast<Risc::OpSize>(Risc::Op::LoadInd), 13, 8, 23,  // lw T9, 8(A3)
      static_cast<Risc::OpSize>(Risc::Op::Add), 14, 12, 13,     // add T10, T8, T9
      static_cast<Risc::OpSize>(Risc::Op::LoadAddr), 25, 0, 1, // lea A5, 0(R1)
      static_cast<Risc::OpSize>(Risc::Op::Store), 14, 16, 25,    // sw T10, 16(A5)
      static_cast<Risc::OpSize>(Risc::Op::LoadAddr), 24, 0, 1, // lea A4, 0(R1)
      static_cast<Risc::OpSize>(Risc::Op::LoadInd), 15, 16, 24,  // lw T11, 16(A4)
      static_cast<Risc::OpSize>(Risc::Op::LoadInd), 16, 8, 0,   // lw T12, 8(R0)
      static_cast<Risc::OpSize>(Risc::Op::Add), 18, 15, 16,     // add T14, T11, T12
      static_cast<Risc::OpSize>(Risc::Op::Store), 18, 16, 0,    // sw T14, 16(R0)
      static_cast<Risc::OpSize>(Risc::Op::Jump),
  };
  // clang-format on

  util::Timer<std::milli> watch;

  watch.Start();

  Risc::Interp(frame, ops);

  watch.Stop();
  std::cout << "q.a: " << ((S *)&frame[1])->a << std::endl;
  std::cout << "q.b: " << ((S *)&frame[1])->b << std::endl;
  std::cout << "q.c: " << ((S *)&frame[1])->c << std::endl;
  std::cout << "p.a: " << param.a << std::endl;
  std::cout << "p.b: " << param.b << std::endl;
  std::cout << "p.c: " << param.c << std::endl;
  std::cout << "Duration: " << watch.elapsed() << " ms" << std::endl;
}

TEST_F(RiscVsCiscTest, Risc2) {
  S param{.a = 0, .b = 1, .c = 0};

  u64 frame[] = {
      (u64)&param,  // L0: param *S
      0,            // L1_1: local q S
      0,            // L1_2: local q S
      0,            // L1_3: local q S
      0,            // T0
      0,            // T1
      0,            // T2
      0,            // T3
      0,            // T4
      0,            // T5
      0,            // T6
      0,            // T7
      0,            // T8
  };

  // clang-format off
  u8 ops[] = {
      static_cast<u8>(Risc2::Op::LoadAddr), 4, 1,       // la T0, L1
      static_cast<u8>(Risc2::Op::StoreImm), 4, 1,       // si (T0), 1
      static_cast<u8>(Risc2::Op::LoadAddr), 4, 1,       // ll T0, L1
      static_cast<u8>(Risc2::Op::Lea), 5, 4, 8,         // lea T1, T0, 8
      static_cast<u8>(Risc2::Op::StoreImm), 5, 2,       // si (T1), 2
      static_cast<u8>(Risc2::Op::StoreImm), 0, 3,       // si (L0), 3
      static_cast<u8>(Risc2::Op::Lea), 4, 0, 8,         // lea T0, L0, 8
      static_cast<u8>(Risc2::Op::StoreImm), 4, 4,       // si (T0), 4
      // q.c = p.a + p.b + q.a + q.b
      static_cast<u8>(Risc2::Op::Load), 4, 0,           // lw T0, (L0)
      static_cast<u8>(Risc2::Op::Lea), 5, 0, 8,         // lea T1, L0, 8
      static_cast<u8>(Risc2::Op::Load), 6, 5,           // lw T2, (T1)
      static_cast<u8>(Risc2::Op::Add), 5, 4, 6,         // add T1, T0, T2 (p.a+p.b)
      static_cast<u8>(Risc2::Op::LoadAddr), 4, 1,       // la T0, L1
      static_cast<u8>(Risc2::Op::Load), 6, 4,           // lw T2, (T0)
      static_cast<u8>(Risc2::Op::LoadAddr), 4, 1,       // la T0, L1
      static_cast<u8>(Risc2::Op::Lea), 7, 4, 8,         // lea T3, T0, 8
      static_cast<u8>(Risc2::Op::Load), 8, 7,           // lw T4, (T3)
      static_cast<u8>(Risc2::Op::Add), 9, 8, 6,         // add T5, T4, T2
      static_cast<u8>(Risc2::Op::Add), 10, 9, 5,        // add T6, T5, T1
      static_cast<u8>(Risc2::Op::LoadAddr), 4, 1,       // la T0, L1
      static_cast<u8>(Risc2::Op::Lea), 5, 4, 16,        // lea T1, T0, 16
      static_cast<u8>(Risc2::Op::Store), 5, 10,         // sw (T1), T6
      // p.c = q.c + p.b
      static_cast<u8>(Risc2::Op::LoadAddr), 4, 1,       // la T0, L1
      static_cast<u8>(Risc2::Op::Lea), 5, 4, 16,        // lea T1, T0, 16
      static_cast<u8>(Risc2::Op::Load), 6, 5,           // lw T2, (T1)
      static_cast<u8>(Risc2::Op::Lea), 4, 0, 8,         // lea T0, L0, 8
      static_cast<u8>(Risc2::Op::Load), 5, 4,           // lw T1, (T0)
      static_cast<u8>(Risc2::Op::Add), 7, 6, 5,         // add T3, T2, T1
      static_cast<u8>(Risc2::Op::Lea), 4, 0, 16,        // lea T0, L0, 8
      static_cast<u8>(Risc2::Op::Store), 4, 7,          // sw (T0), T3
      static_cast<u8>(Risc2::Op::Jump),
  };
  // clang-format on

  util::Timer<std::milli> watch;

  watch.Start();

  Risc2::Interp(frame, ops);

  watch.Stop();
  std::cout << "q.a: " << ((S *)&frame[1])->a << std::endl;
  std::cout << "q.b: " << ((S *)&frame[1])->b << std::endl;
  std::cout << "q.c: " << ((S *)&frame[1])->c << std::endl;
  std::cout << "p.a: " << param.a << std::endl;
  std::cout << "p.b: " << param.b << std::endl;
  std::cout << "p.c: " << param.c << std::endl;
  std::cout << "Duration: " << watch.elapsed() << " ms" << std::endl;
}

TEST_F(RiscVsCiscTest, Cisc) {
  S param{.a = 0, .b = 1, .c = 0};

  u64 frame[] = {
      (u64)&param,  // T0: param *S
      0,            // T1: local q S
      0,            // T2: local q S
      0,            // T3: local q S
      0,            // T4: &q.a
      0,            // T5: &q.b
      0,            // T6: &p->a
      0,            // T7: &p->b
      0,            // T8: &p->a
      0,            // T9: &p->b
      0,            // T10: t1 = p.a + p.b
      0,            // T11: &q.a
      0,            // T12: t2 = t1 + q.a
      0,            // T13: &q.b
      0,            // T14: &q.c
      0,            // T15: &q.c
      0,            // T16: &p.b
      0,            // T17: &p.c
  };

  // clang-format off
  u8 ops[] = {
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(4), Cisc::AsReg<false>(1), 0,
      static_cast<u8>(Cisc::Op::LoadImm), Cisc::AsReg<true>(4), 1,
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(5), Cisc::AsReg<false>(1), 8,
      static_cast<u8>(Cisc::Op::LoadImm), Cisc::AsReg<true>(5), 2,

      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(6), Cisc::AsReg<true>(0), 0,
      static_cast<u8>(Cisc::Op::LoadImm), Cisc::AsReg<true>(6), 3,
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(7), Cisc::AsReg<true>(0), 8,
      static_cast<u8>(Cisc::Op::LoadImm), Cisc::AsReg<true>(7), 4,

      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(8), Cisc::AsReg<true>(0), 0,
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(9), Cisc::AsReg<true>(0), 8,
      static_cast<u8>(Cisc::Op::Add), Cisc::AsReg<false>(10), Cisc::AsReg<true>(8), Cisc::AsReg<true>(9),

      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(11), Cisc::AsReg<false>(1), 0,
      static_cast<u8>(Cisc::Op::Add), Cisc::AsReg<false>(12), Cisc::AsReg<false>(10), Cisc::AsReg<true>(11),

      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(13), Cisc::AsReg<false>(1), 8,
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(14), Cisc::AsReg<false>(1), 16,
      static_cast<u8>(Cisc::Op::Add), Cisc::AsReg<true>(14), Cisc::AsReg<false>(12), Cisc::AsReg<true>(13),

      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(15), Cisc::AsReg<false>(1), 16,
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(16), Cisc::AsReg<true>(0), 8,
      static_cast<u8>(Cisc::Op::Lea), Cisc::AsReg<false>(17), Cisc::AsReg<true>(0), 16,
      static_cast<u8>(Cisc::Op::Add), Cisc::AsReg<true>(17), Cisc::AsReg<true>(15), Cisc::AsReg<true>(16),

      static_cast<u8>(Cisc::Op::Jump),
  };
  // clang-format on

  util::Timer<std::milli> watch;

  watch.Start();

  Cisc::Interp(frame, ops);

  watch.Stop();
  std::cout << "q.a: " << ((S *)&frame[1])->a << std::endl;
  std::cout << "q.b: " << ((S *)&frame[1])->b << std::endl;
  std::cout << "q.c: " << ((S *)&frame[1])->c << std::endl;
  std::cout << "p.a: " << param.a << std::endl;
  std::cout << "p.b: " << param.b << std::endl;
  std::cout << "p.c: " << param.c << std::endl;
  std::cout << "Duration: " << watch.elapsed() << " ms" << std::endl;
}

}  // namespace tpl::vm::test
