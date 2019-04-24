#pragma once

#include <ast/type.h>
#include <util/macros.h>
#include <string>
#include <vector>
#include "compiler/code_context.h"

namespace tpl::compiler {

// Forward declare
class CodeGen;

/**
 * A CppProxyMember defines an access proxy to a member defined in a C++ class.
 * Users can use this class to generate code to load values from and store
 * values into member variables of C++ classes/structs available at runtime.
 * Each member is defined by a slot position in the C++ struct. Slots are
 * zero-based ordinal numbers assigned to fields increasing order of appearance
 * in the struct/class.
 */
class CppProxyMember {
 public:
  explicit CppProxyMember(uint32_t slot) noexcept : slot_(slot) {}

  /**
   * Load this member field from the provided struct pointer.
   *
   * @param codegen The codegen instance
   * @param obj_ptr A pointer to a runtime C++ struct
   * @return The value of the field
   */
  llvm::Value *Load(CodeGen &codegen, llvm::Value *obj_ptr) const;

  /**
   * Store the provided value into this member field of the provided struct.
   *
   * @param codegen The codegen instance
   * @param obj_ptr A pointer to a runtime C++ struct
   * @param val The value of the field
   */
  void Store(CodeGen &codegen, llvm::Value *obj_ptr, llvm::Value *val) const;

 private:
  // The slot position
  uint32_t slot_;
};

/**
 * The main API used to generate code in Terrier.
 */
class CodeGen {
 public:
  /// Constructor and destructor
  explicit CodeGen(CodeContext &code_context);
  ~CodeGen() {
    for (auto val : allocated_vals) {
      delete val;
    }
  }

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CodeGen);

  /// Type wrappers
  Type *BoolType() const { return code_context_.bool_type_; }
  Type *Int8Type() const { return code_context_.int8_type_; }
  Type *ByteType() const { return Int8Type(); }
  Type *Int16Type() const { return code_context_.int16_type_; }
  Type *Int32Type() const { return code_context_.int32_type_; }
  Type *Int64Type() const { return code_context_.int64_type_; }
  Type *DoubleType() const { return code_context_.double_type_; }
  Type *VoidType() const { return code_context_.void_type_; }
  Type *VoidPtrType() const { return code_context_.void_ptr_type_; }
  // PointerType *CharPtrType() const { return code_context_.char_ptr_type_; }
  // /Generate a call to the function with the provided name and arguments
  void CallFunc(Function *fn, std::initializer_list<Value *> args);

  template <typename T>
  Value *Call(const T &proxy, const std::vector<Value *> &args) {
    return CallFunc(proxy.GetFunction(*this), args);
  }

  template <typename T>
  Value *Load(const T &loader, Value *obj_ptr) {
    return loader.Load(*this, obj_ptr);
  }

  template <typename T>
  void Store(const T &storer, Value *obj_ptr, Value *val) {
    storer.Store(*this, obj_ptr, val);
  }

  //===--------------------------------------------------------------------===//
  // Constant Generators
  //===--------------------------------------------------------------------===//
  Constant *ConstBool(bool val) const;
  Constant *Const8(int8_t val) const;
  Constant *Const16(int16_t val) const;
  Constant *Const32(int32_t val) const;
  Constant *Const64(int64_t val) const;
  Constant *ConstDouble(double val) const;
  Value *AllocateVariable(Type *type, const std::string &name);

  //===--------------------------------------------------------------------===//
  // C/C++ standard library functions
  //===--------------------------------------------------------------------===//
  llvm::Value *Printf(const std::string &format,
                      const std::vector<llvm::Value *> &args);
  llvm::Value *Memcmp(llvm::Value *ptr1, llvm::Value *ptr2, llvm::Value *len);
  llvm::Value *Sqrt(llvm::Value *val);

  //===--------------------------------------------------------------------===//
  // Arithmetic with overflow logic - These methods perform the desired math op,
  // on the provided left and right argument and return the result of the op
  // and set the overflow_but out-parameter. It is up to the caller to decide
  // how to handle an overflow.
  //===--------------------------------------------------------------------===//
  Value *CallAddWithOverflow(Value *left, Value *right, Value *&overflow_bit);
  Value *CallSubWithOverflow(Value *left, Value *right, Value *&overflow_bit);
  Value *CallMulWithOverflow(Value *left, Value *right, Value *&overflow_bit);
  void ThrowIfOverflow(Value *overflow) const;
  void ThrowIfDivideByZero(Value *divide_by_zero) const;

  //===--------------------------------------------------------------------===//
  // Function lookup and registration
  //===--------------------------------------------------------------------===//
  Type *LookupType(const std::string &name) const;
  Function *LookupBuiltin(const std::string &name) const;
  Function *RegisterBuiltin(const std::string &fn_name,
                            std::vector<Value *> params);

  /// Get the runtime state function argument
  Value *GetState() const;

  /// Return the size of the given type in bytes (returns 1 when size < 1 byte)
  uint64_t SizeOf(llvm::Type *type) const;
  uint64_t ElementOffset(llvm::Type *type, uint32_t element_idx) const;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // llvm::LLVMContext &GetContext() const { return code_context_.GetContext();
  // }

  CodeContext &GetCodeContext() const { return code_context_; }

  Function *GetCurrentFunction() const {
    return code_context_.GetCurrentFunction();
  }

  //===--------------------------------------------------------------------===//
  // DEBUG OUTPUT
  //===--------------------------------------------------------------------===//

  static std::string Dump(const llvm::Value *value);
  static std::string Dump(llvm::Type *type);

 private:
  friend class Hash;
  friend class Value;
  friend class OAHashTable;

  // Get the LLVM module
  llvm::Module &GetModule() const { return code_context_.GetModule(); }

  // Get the LLVM IR Builder (also accessible through the -> operator overload)
  CodeBuilder<> &GetBuilder() const { return code_context_.GetBuilder(); }

 private:
  // The context/module where all the code this class produces goes
  CodeContext &code_context_;

  std::vector<Value *&> allocated_vals;
};

}  // namespace tpl::compiler
