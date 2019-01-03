#include "vm/llvm_engine.h"

#include "llvm/ADT/StringMap.h"
#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm/Analysis/TargetTransformInfo.h"
#include "llvm/Bitcode/BitcodeReader.h"
#include "llvm/ExecutionEngine/RuntimeDyld.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/MC/MCContext.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/Path.h"
#include "llvm/Support/SmallVectorMemoryBuffer.h"
#include "llvm/Support/TargetRegistry.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/IPO/AlwaysInliner.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/Transforms/Scalar.h"

#include "ast/type.h"
#include "logging/logger.h"
#include "vm/bytecode_module.h"
#include "vm/bytecode_traits.h"

namespace tpl::vm {

// ---------------------------------------------------------
// TPL's Jit Memory Manager
// ---------------------------------------------------------

class LLVMEngine::TPLMemoryManager : public llvm::SectionMemoryManager {
 public:
  llvm::JITSymbol findSymbol(const std::string &name) override {
    LOG_DEBUG("Resolving symbol '{}' ...", name);

    if (const auto iter = symbols_.find(name); iter != symbols_.end()) {
      LOG_DEBUG("Symbol '{}' found in cache ...", name);
      return llvm::JITSymbol(iter->second);
    }

    LOG_DEBUG("Symbol '{}' not found in cache, checking process ...", name);

    auto sym = llvm::SectionMemoryManager::findSymbol(name);
    TPL_ASSERT(sym.getAddress(), "Resolved symbol has no address!");
    symbols_[name] = {sym.getAddress().get(), sym.getFlags()};
    return sym;
  }

 private:
  std::unordered_map<std::string, llvm::JITEvaluatedSymbol> symbols_;
};

// ---------------------------------------------------------
// TPL Type to LLVM Type
// ---------------------------------------------------------

/// A handy class that maps TPL types to LLVM types
class LLVMEngine::TypeMap {
 public:
  explicit TypeMap(llvm::Module *module) : module_(module) {
    llvm::LLVMContext &ctx = module->getContext();
    type_map_["nil"] = llvm::Type::getVoidTy(ctx);
    type_map_["bool"] = llvm::Type::getInt1Ty(ctx);
    type_map_["int8"] = llvm::Type::getInt8Ty(ctx);
    type_map_["int16"] = llvm::Type::getInt16Ty(ctx);
    type_map_["int32"] = llvm::Type::getInt32Ty(ctx);
    type_map_["int64"] = llvm::Type::getInt64Ty(ctx);
    type_map_["uint8"] = llvm::Type::getInt8Ty(ctx);
    type_map_["uint16"] = llvm::Type::getInt16Ty(ctx);
    type_map_["uint32"] = llvm::Type::getInt32Ty(ctx);
    type_map_["uint64"] = llvm::Type::getInt64Ty(ctx);
    type_map_["float32"] = llvm::Type::getFloatTy(ctx);
    type_map_["float64"] = llvm::Type::getDoubleTy(ctx);
  }

  /// No copying or moving this class
  DISALLOW_COPY_AND_MOVE(TypeMap);

  llvm::Type *VoidType() { return type_map_["nil"]; }
  llvm::Type *BoolType() { return type_map_["bool"]; }
  llvm::Type *Int8Type() { return type_map_["int8"]; }
  llvm::Type *Int16Type() { return type_map_["int16"]; }
  llvm::Type *Int32Type() { return type_map_["int32"]; }
  llvm::Type *Int64Type() { return type_map_["int64"]; }
  llvm::Type *UInt8Type() { return type_map_["uint8"]; }
  llvm::Type *UInt16Type() { return type_map_["uint16"]; }
  llvm::Type *UInt32Type() { return type_map_["uint32"]; }
  llvm::Type *UInt64Type() { return type_map_["uint64"]; }
  llvm::Type *Float32Type() { return type_map_["float32"]; }
  llvm::Type *Float64Type() { return type_map_["float64"]; }

  llvm::Type *GetLLVMType(const ast::Type *type);

 private:
  llvm::Module *module() { return module_; }

 private:
  llvm::Module *module_;
  std::unordered_map<std::string, llvm::Type *> type_map_;
};

llvm::Type *LLVMEngine::TypeMap::GetLLVMType(const ast::Type *type) {
  llvm::Type *llvm_type = nullptr;
  switch (type->kind()) {
    case ast::Type::Kind::BoolType:
    case ast::Type::Kind::IntegerType:
    case ast::Type::Kind::FloatType:
    case ast::Type::Kind::NilType: {
      llvm_type = type_map_[type->ToString()];
      break;
    }
    case ast::Type::Kind::PointerType: {
      auto *ptr_type = type->As<ast::PointerType>();
      llvm_type = llvm::PointerType::get(GetLLVMType(ptr_type->base()), 0);
      break;
    }
    case ast::Type::Kind::ArrayType: {
      auto *arr_type = type->As<ast::ArrayType>();
      llvm::Type *elem_type = GetLLVMType(arr_type->element_type());
      llvm_type = llvm::PointerType::get(elem_type, 0);
      break;
    }
    case ast::Type::Kind::MapType: {
      // TODO: me
      break;
    }
    case ast::Type::Kind::StructType: {
      llvm::SmallVector<llvm::Type *, 8> fields;
      for (const auto &field : type->As<ast::StructType>()->fields()) {
        fields.push_back(GetLLVMType(field.type));
      }
      llvm_type = llvm::StructType::create(fields);

      break;
    }
    case ast::Type::Kind::FunctionType: {
      // TODO: me
      break;
    }
    case ast::Type::Kind::InternalType: {
      std::string name = type->As<ast::InternalType>()->name().data();
      // Try "struct" and "class" prefixes
      if (auto *t = module()->getTypeByName("struct." + name); t != nullptr) {
        llvm_type = t;
      } else if (t = module()->getTypeByName("class." + name); t != nullptr) {
        llvm_type = t;
      } else {
        LOG_ERROR("Could not find LLVM type for TPL type '{}'", name);
      }
      break;
    }
    case ast::Type::Kind::SqlType: {
      auto *sql_type = type->As<ast::SqlType>();
      switch (sql_type->sql_type().type_id()) {
        case sql::TypeId::Boolean:
        case sql::TypeId::SmallInt:
        case sql::TypeId::Integer:
        case sql::TypeId::BigInt: {
          llvm_type = module()->getTypeByName("struct.tpl::sql::Integer");
          break;
        }
        default: { break; }
      }
      break;
    }
  }

  TPL_ASSERT(llvm_type != nullptr, "No LLVM type found!");

  return llvm_type;
}

// ---------------------------------------------------------
// Function Helper
// ---------------------------------------------------------

/// This class helps construction of an LLVM function from a TPL function
class LLVMEngine::FunctionHelper {
 public:
  FunctionHelper(const FunctionInfo &func_info, llvm::Function *func,
                 TypeMap *type_map,
                 llvm::IRBuilder<llvm::ConstantFolder,
                                 llvm::IRBuilderDefaultInserter> &ir_builder);

  /// Given a reference to a local variable in a function, return the LLVM
  /// equivalent.
  /// \param var
  /// \return
  llvm::Value *GetArgumentById(LocalVar var);

 private:
  llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
      &ir_builder_;
  std::unordered_map<u32, llvm::Value *> params_;
  std::unordered_map<u32, llvm::Value *> locals_;
};

LLVMEngine::FunctionHelper::FunctionHelper(const FunctionInfo &func_info,
                                           llvm::Function *func,
                                           TypeMap *type_map,
                                           llvm::IRBuilder<> &ir_builder)
    : ir_builder_(ir_builder) {
  // Setup locals
  u32 local_idx = 0;
  auto arg_iter = func->arg_begin();
  for (local_idx = 0; local_idx < func_info.num_params(); local_idx++) {
    const auto &param = func_info.locals()[local_idx];
    params_[param.offset()] = &*arg_iter;
  }

  for (; local_idx < func_info.locals().size(); local_idx++) {
    const auto &local = func_info.locals()[local_idx];
    auto *val = ir_builder.CreateAlloca(type_map->GetLLVMType(local.type()));
    locals_[local.offset()] = val;
  }
}

llvm::Value *LLVMEngine::FunctionHelper::GetArgumentById(LocalVar var) {
  if (auto iter = params_.find(var.GetOffset()); iter != params_.end()) {
    return iter->second;
  }

  if (auto iter = locals_.find(var.GetOffset()); iter != locals_.end()) {
    llvm::Value *val = iter->second;

    if (var.GetAddressMode() == LocalVar::AddressMode::Value) {
      val = ir_builder_.CreateLoad(val);
    }

    return val;
  }

  LOG_ERROR("No variable found at offset {}", var.GetOffset());

  return nullptr;
}

// ---------------------------------------------------------
// Compiled Module Builder
// ---------------------------------------------------------

/// A builder to create a compilation unit. We need this because a compilation
/// unit is immutable after creation.
class LLVMEngine::CompiledModuleBuilder {
 public:
  CompiledModuleBuilder(const CompilerOptions &options,
                        const vm::BytecodeModule &tpl_module);

  /// No copying or moving this class
  DISALLOW_COPY_AND_MOVE(CompiledModuleBuilder);

  /// Create function declarations for all functions declared in the TPL
  /// bytecode module
  /// \param bytecode_module The bytecode module
  void DeclareFunctions();

  /// Generate LLVM function implementations for all functions defined in the
  /// TPL bytecode module
  /// \param bytecode_module The bytecode module
  void DefineFunctions();

  /// Verify that all generated code is good
  void Verify();

  /// Clean up the code
  void RunSimpleOptimizations();

  /// Optimize the generate code
  void RunAggressiveOptimizations();

  /// Perform finalization logic create a compilation unit
  /// \return A compilation unit housing all LLVM bitcode for the module
  std::unique_ptr<CompiledModule> Finalize();

  /// Print the contents of the module to a string and return it
  /// \return Stringified module contents
  std::string DumpModule() const;

 private:
  /// Define the body of the function \ref func_info
  /// \param func_info The function to define
  /// \param ir_builder LLVM's IR builder
  void DefineFunction(
      const FunctionInfo &func_info,
      llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
          &ir_builder);

  /// Lookup the handler function for the given bytecode
  /// \param bytecode The bytecode whose handler to lookup
  /// \return
  llvm::Function *LookupBytecodeHandler(Bytecode bytecode) const;

  /// Generate a raw object file in memory
  /// \return A buffer containing the object code for the compilation unit
  std::unique_ptr<llvm::MemoryBuffer> EmitObject();

  /// Write the object out
  void PersistObjectToFile(const llvm::MemoryBuffer &obj_buffer);

  // -----------------------------------------------------
  // Accessors
  // -----------------------------------------------------

  const CompilerOptions &options() const { return options_; }

  const vm::BytecodeModule &tpl_module() const { return tpl_module_; }

  llvm::TargetMachine *target_machine() { return target_machine_.get(); }

  llvm::LLVMContext &context() { return *context_; }

  llvm::Module *module() { return llvm_module_.get(); }

  const llvm::Module *module() const { return llvm_module_.get(); }

  TypeMap *type_map() { return type_map_.get(); }

 private:
  const CompilerOptions &options_;
  const vm::BytecodeModule &tpl_module_;
  std::unique_ptr<llvm::TargetMachine> target_machine_;
  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::Module> llvm_module_;
  std::unique_ptr<TypeMap> type_map_;
};

// ---------------------------------------------------------
// Compilation Unit Builder
// ---------------------------------------------------------

LLVMEngine::CompiledModuleBuilder::CompiledModuleBuilder(
    const CompilerOptions &options, const vm::BytecodeModule &tpl_module)
    : options_(options),
      tpl_module_(tpl_module),
      context_(std::make_unique<llvm::LLVMContext>()) {
  /*
   * First, we create a suitable target machine for LLVM to use when we JIT
   * TPL programs. At the moment, we rely on LLVM to discover all CPU features
   * e.g., AVX2 or AVX512, and we make no assumptions about symbol relocations.
   * All this is to say that we leave much of these configurables as defaults
   * for the purposes of getting the basics working. We can revisit at a later
   * timer.
   */

  {
    std::string triple = llvm::sys::getProcessTriple();
    std::string error;
    auto *target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (target == nullptr) {
      LOG_ERROR("LLVM: Unable to find target with triple {}", triple);
      return;
    }

    // Collect CPU features
    llvm::StringMap<bool> feature_map;
    if (bool success = llvm::sys::getHostCPUFeatures(feature_map); !success) {
      LOG_ERROR("LLVM: Unable to find all CPU features");
      return;
    }

    llvm::SubtargetFeatures target_features;
    for (const auto &entry : feature_map) {
      target_features.AddFeature(entry.getKey(), entry.getValue());
    }

    LOG_DEBUG("LLVM: Discovered CPU features: {}", target_features.getString());

    std::string cpu = llvm::sys::getHostCPUName();
    llvm::TargetOptions target_options;
    llvm::Optional<llvm::Reloc::Model> reloc;
    target_machine_ =
        std::unique_ptr<llvm::TargetMachine>(target->createTargetMachine(
            triple, cpu, target_features.getString(), target_options, reloc));
    TPL_ASSERT(target_machine_ != nullptr,
               "LLVM: Unable to find a suitable target machine!");
  }

  /*
   * We've built the target machine we use to generate machine code. Now, we
   * load the pre-compiled bytecode module containing all the TPL bytecode
   * logic. We add the functions we're about to compile into this module. This
   * module forms the unit of JIT.
   */

  {
    auto memory_buffer =
        llvm::MemoryBuffer::getFile(options.GetBytecodeHandlersBcPath());
    if (auto error = memory_buffer.getError()) {
      LOG_ERROR("There was an error loading the handler bytecode: {}",
                error.message());
    }

    auto module = llvm::parseBitcodeFile(*(memory_buffer.get()), context());
    if (!module) {
      auto error = llvm::toString(module.takeError());
      LOG_ERROR("{}", error);
      throw std::runtime_error(error);
    }

    llvm_module_ = std::move(module.get());
    llvm_module_->setModuleIdentifier(tpl_module.name());
    llvm_module_->setSourceFileName(tpl_module.name());
  }

  type_map_ = std::make_unique<TypeMap>(llvm_module_.get());
}

void LLVMEngine::CompiledModuleBuilder::DeclareFunctions() {
  /*
   * For each TPL function in the bytecode module we build an equivalent LLVM
   * function declaration.
   */
  for (const auto &func_info : tpl_module().functions()) {
    llvm::SmallVector<llvm::Type *, 8> param_types;

    for (u32 i = 0; i < func_info.num_params(); i++) {
      const ast::Type *type = func_info.locals()[i].type();
      param_types.push_back(type_map()->GetLLVMType(type));
    }

    llvm::FunctionType *func_type =
        llvm::FunctionType::get(type_map()->VoidType(), param_types, false);

    module()->getOrInsertFunction(func_info.name(), func_type);
  }
}

llvm::Function *LLVMEngine::CompiledModuleBuilder::LookupBytecodeHandler(
    Bytecode bytecode) const {
  const char *handler_name = Bytecodes::GetBytecodeHandlerName(bytecode);
  llvm::Function *func = module()->getFunction(handler_name);
#ifndef NDEBUG
  if (func == nullptr) {
    auto error =
        fmt::format("No bytecode handler function '{}' for bytecode {}",
                    handler_name, Bytecodes::ToString(bytecode));
    LOG_ERROR("{}", error);
    throw std::runtime_error(error);
  }
#endif
  return func;
}

void LLVMEngine::CompiledModuleBuilder::DefineFunction(
    const FunctionInfo &func_info,
    llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
        &ir_builder) {
  llvm::LLVMContext &ctx = ir_builder.getContext();
  llvm::Function *func = module()->getFunction(func_info.name());
  llvm::BasicBlock *entry = llvm::BasicBlock::Create(ctx, "EntryBB", func);

  ir_builder.SetInsertPoint(entry);

  /*
   * Before we can generate LLVM IR, we need to build a control-flow graph (CFG)
   * for the function. We do this construction directly from the TPL bytecode
   * using a vanilla DFS and produce an ordered map ('blocks') from bytecode
   * position to an LLVM basic block. Each entry in the map indicates the start
   * of a basic block.
   */

  llvm::SmallVector<std::size_t, 16> bb_begin_positions = {0};
  std::map<std::size_t, llvm::BasicBlock *> blocks = {{0, entry}};

  for (auto iter = tpl_module().BytecodeForFunction(func_info);
       !bb_begin_positions.empty();) {
    std::size_t begin_pos = bb_begin_positions.back();
    bb_begin_positions.pop_back();

    for (iter.SetPosition(begin_pos); !iter.Done(); iter.Advance()) {
      Bytecode bytecode = iter.CurrentBytecode();

      if (Bytecodes::IsTerminal(bytecode)) {
        if (Bytecodes::IsJump(bytecode)) {
          // Unconditional Jump
          std::size_t branch_target_pos =
              iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 0) +
              iter.GetJumpOffsetOperand(0);

          if (blocks.find(branch_target_pos) == blocks.end()) {
            blocks[branch_target_pos] = nullptr;
            bb_begin_positions.push_back(branch_target_pos);
          }
        }

        break;
      }

      if (Bytecodes::IsJump(bytecode)) {
        // Conditional Jump
        std::size_t fallthrough_pos =
            iter.GetPosition() + iter.CurrentBytecodeSize();

        if (blocks.find(fallthrough_pos) == blocks.end()) {
          bb_begin_positions.push_back(fallthrough_pos);
          blocks[fallthrough_pos] = nullptr;
        }

        std::size_t branch_target_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 1) +
            iter.GetJumpOffsetOperand(1);

        if (blocks.find(branch_target_pos) == blocks.end()) {
          bb_begin_positions.push_back(branch_target_pos);
          blocks[branch_target_pos] = nullptr;
        }

        break;
      }
    }
  }

  // Set block names
  {
    u32 i = 1;
    for (auto &[_, block] : blocks) {
      if (block == nullptr) {
        block = llvm::BasicBlock::Create(ctx, "BB" + std::to_string(i++), func);
      }
    }
  }

#ifndef NDEBUG
  LOG_DEBUG("Found blocks:");
  for (auto &[pos, block] : blocks) {
    LOG_DEBUG("  Block {} @ {:x}", block->getName().str(), pos);
  }
#endif

  /*
   * We can define the function now. LLVM IR generation happens by iterating
   * over the function's bytecode simultaneously with the ordered list of basic
   * block start positions ('blocks'). Each TPL bytecode is converted to a
   * function call into a pre-compiled TPL bytecode handler. However, many of
   * these calls will get inlined away during optimization. If the current
   * bytecode position matches the position of a new basic block, a branch
   * instruction is generated automatically (either conditional or not depending
   * on context) into the new block, and the IR builder position shifts to the
   * new block.
   */

  auto block_iter = ++blocks.cbegin();

  FunctionHelper function_helper(func_info, func, type_map(), ir_builder);

  bool last_was_jump = false;
  for (auto iter = tpl_module().BytecodeForFunction(func_info); !iter.Done();
       iter.Advance()) {
    Bytecode bytecode = iter.CurrentBytecode();

    if (iter.GetPosition() == block_iter->first) {
      if (!last_was_jump) {
        ir_builder.CreateBr(block_iter->second);
      }
      ir_builder.SetInsertPoint(block_iter->second);
      ++block_iter;
    }

    // Collect arguments
    llvm::SmallVector<llvm::Value *, 8> args;
    for (u32 i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
      switch (Bytecodes::GetNthOperandType(bytecode, i)) {
        case OperandType::None: {
          break;
        }
        case OperandType::Imm1: {
          args.push_back(llvm::ConstantInt::get(
              type_map()->Int8Type(), iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::Imm2: {
          args.push_back(llvm::ConstantInt::get(
              type_map()->Int16Type(), iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::Imm4: {
          args.push_back(llvm::ConstantInt::get(
              type_map()->Int32Type(), iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::Imm8: {
          args.push_back(llvm::ConstantInt::get(
              type_map()->Int64Type(), iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::UImm2: {
          args.push_back(llvm::ConstantInt::get(
              type_map()->UInt16Type(), iter.GetUnsignedImmediateOperand(i),
              false));
          break;
        }
        case OperandType::JumpOffset: {
          break;
        }
        case OperandType::UImm4: {
          args.push_back(llvm::ConstantInt::get(
              type_map()->UInt32Type(), iter.GetUnsignedImmediateOperand(i),
              false));
          break;
        }
        case OperandType::Local: {
          LocalVar local = iter.GetLocalOperand(i);
          args.push_back(function_helper.GetArgumentById(local));
          break;
        }
        case OperandType::LocalCount: {
          break;
        }
      }
    }

    // Find function to call
    llvm::Function *callee = nullptr;
    if (Bytecodes::IsCall(bytecode)) {
    } else {
      callee = LookupBytecodeHandler(bytecode);
    }

    // Clean up call arguments
    TPL_ASSERT(callee != nullptr, "Couldn't find function!");
    TPL_ASSERT(args.size() == callee->arg_size(), "Argument mismatch!");
    for (u32 i = 0; i < args.size(); i++) {
      auto *arg = callee->arg_begin() + i;
      if (args[i]->getType() != arg->getType()) {
        if (args[i]->getType()->isIntegerTy()) {
          args[i] = ir_builder.CreateIntCast(args[i], arg->getType(), true);
        } else if (args[i]->getType()->isPointerTy()) {
          args[i] = ir_builder.CreatePointerCast(args[i], arg->getType());
        }
      }
    }

    // Issue call
    llvm::Value *ret = ir_builder.CreateCall(callee, args);

    last_was_jump = false;
    if (Bytecodes::IsJump(bytecode)) {
      last_was_jump = true;
      if (!Bytecodes::IsTerminal(bytecode)) {
        std::size_t fallthrough_bb_pos =
            iter.GetPosition() + iter.CurrentBytecodeSize();
        std::size_t branch_target_bb_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 1) +
            iter.GetJumpOffsetOperand(1);
        ret = ir_builder.CreateTrunc(ret, llvm::Type::getInt1Ty(ctx));
        ir_builder.CreateCondBr(ret, blocks[fallthrough_bb_pos],
                                blocks[branch_target_bb_pos]);
      } else {
        std::size_t branch_target_bb_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 0) +
            iter.GetJumpOffsetOperand(0);
        ir_builder.CreateBr(blocks[branch_target_bb_pos]);
      }
    }
  }

  // Done
  ir_builder.CreateRetVoid();
}

void LLVMEngine::CompiledModuleBuilder::DefineFunctions() {
  /*
   * We iterate over all the TPL functions defined in the module and generate
   * their LLVM equivalents into the current module.
   */

  llvm::IRBuilder<> ir_builder(context());
  for (const auto &func_info : tpl_module().functions()) {
    DefineFunction(func_info, ir_builder);
  }
}

void LLVMEngine::CompiledModuleBuilder::Verify() {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  if (bool has_error = llvm::verifyModule(*module(), &ostream); has_error) {
    // TODO(pmenon): Do something more here ...
    LOG_ERROR("ERROR IN MODULE: {}", result);
  }
}

void LLVMEngine::CompiledModuleBuilder::RunSimpleOptimizations() {
  /*
   * This function ensures all bytecode handlers marked 'always_inline' are
   * inlined into the main TPL program. After this inlining, we clean up any
   * unused functions.
   */

  llvm::legacy::PassManager pass_manager;
  pass_manager.add(llvm::createAlwaysInlinerLegacyPass());
  pass_manager.add(llvm::createGlobalDCEPass());
  pass_manager.run(*module());
}

void LLVMEngine::CompiledModuleBuilder::RunAggressiveOptimizations() {
  /*
   * The optimization passes we use are somewhat ad-hoc, but were found to
   * provide a nice balance of performance and compilation times. We use an
   * aggressive function inlining pass followed by a CFG simplification pass
   * that should clean up work done during earlier inlining and DCE work.
   */

  llvm::PassManagerBuilder pm_builder;
  pm_builder.Inliner = llvm::createFunctionInliningPass(3, 0, false);

  /*
   * The function optimization passes ...
   */

  llvm::legacy::FunctionPassManager function_pm(module());
  function_pm.add(llvm::createTargetTransformInfoWrapperPass(
      target_machine()->getTargetIRAnalysis()));
  function_pm.add(llvm::createCFGSimplificationPass());

  /*
   * The module-level optimization passes ...
   */

  llvm::legacy::PassManager module_pm;
  module_pm.add(llvm::createTargetTransformInfoWrapperPass(
      target_machine()->getTargetIRAnalysis()));

  pm_builder.populateFunctionPassManager(function_pm);
  pm_builder.populateModulePassManager(module_pm);

  // First, run the function-level optimizations
  function_pm.doInitialization();
  for (const auto &func_info : tpl_module().functions()) {
    auto *func = module()->getFunction(func_info.name());
    function_pm.run(*func);
  }
  function_pm.doFinalization();

  // Now run the module-level optimizations
  module_pm.run(*module());
}

std::unique_ptr<LLVMEngine::CompiledModule>
LLVMEngine::CompiledModuleBuilder::Finalize() {
  std::unique_ptr<llvm::MemoryBuffer> obj = EmitObject();

  if (options().ShouldPersistObjectFile()) {
    PersistObjectToFile(*obj);
  }

  return std::make_unique<CompiledModule>(std::move(obj));
}

std::unique_ptr<llvm::MemoryBuffer>
LLVMEngine::CompiledModuleBuilder::EmitObject() {
  /*
   * Generating a raw object file involves running a specialized pass that emits
   * machine code. Thus, all we need to do is create an in-memory buffer to hold
   * the machine code, and use that as the output buffer for the pass.
   */

  llvm::SmallVector<char, 0> obj_buffer;

  {
    // The pass manager we insert the EmitMC pass into
    llvm::legacy::PassManager pass_manager;
    pass_manager.add(new llvm::TargetLibraryInfoWrapperPass(
        target_machine()->getTargetTriple()));
    pass_manager.add(llvm::createTargetTransformInfoWrapperPass(
        target_machine()->getTargetIRAnalysis()));

    llvm::MCContext *mc_ctx;
    llvm::raw_svector_ostream obj_buffer_stream(obj_buffer);
    if (target_machine()->addPassesToEmitMC(pass_manager, mc_ctx,
                                            obj_buffer_stream)) {
      LOG_ERROR("The target LLVM machine cannot emit a file of this type");
      return nullptr;
    }

    // Generate code
    pass_manager.run(*module());
  }

  return std::make_unique<llvm::SmallVectorMemoryBuffer>(std::move(obj_buffer));
}

void LLVMEngine::CompiledModuleBuilder::PersistObjectToFile(
    const llvm::MemoryBuffer &obj_buffer) {
  const std::string file_name = tpl_module().name() + ".to";

  std::error_code error_code;
  llvm::raw_fd_ostream dest(file_name, error_code, llvm::sys::fs::F_None);

  if (error_code) {
    LOG_ERROR("Could not open file: {}", error_code.message());
    return;
  }

  dest.write(obj_buffer.getBufferStart(), obj_buffer.getBufferSize());
  dest.flush();
  dest.close();
}

std::string LLVMEngine::CompiledModuleBuilder::DumpModule() const {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  module()->print(ostream, nullptr);
  return result;
}

// ---------------------------------------------------------
// Compilation Unit
// ---------------------------------------------------------

LLVMEngine::CompiledModule::CompiledModule(
    std::unique_ptr<llvm::MemoryBuffer> object_code)
    : loaded_(false),
      object_code_(std::move(object_code)),
      memory_manager_(std::make_unique<LLVMEngine::TPLMemoryManager>()) {}

// This destructor is needed because we have a unique_ptr to a forward-declared
// TPLMemoryManager class.
LLVMEngine::CompiledModule::~CompiledModule() = default;

void *LLVMEngine::CompiledModule::GetFunctionPointer(
    const std::string &name) const {
  TPL_ASSERT(loaded(), "Compiled module isn't loaded!");

  if (auto iter = functions_.find(name); iter != functions_.end()) {
    return iter->second;
  }

  return nullptr;
}

void LLVMEngine::CompiledModule::Load(const BytecodeModule &module) {
  /*
   * A compiled module can be initialized with or without an in-memory object.
   * If the module does not have an existing in-memory version, one will be
   * loaded from the file system using the module's name and storage directory.
   * If the module has already been read into a memory buffer, it must be
   * loaded and linked into the environment.
   */

  if (loaded()) {
    return;
  }

  if (object_code() == nullptr) {
    llvm::SmallString<128> path;
    if (std::error_code error = llvm::sys::fs::current_path(path)) {
      LOG_ERROR("LLVMEngine: Error reading current path '{}'", error.message());
      return;
    }
    llvm::sys::path::append(path, module.name(), ".to");
    auto file_buffer = llvm::MemoryBuffer::getFile(path);
    if (std::error_code error = file_buffer.getError()) {
      LOG_ERROR("LLVMEngine: Error reading object file '{}'", error.message());
      return;
    }
    object_code_ = std::move(file_buffer.get());
  }

  /*
   * At this point, we have a valid object code buffer containing the contents
   * of the module. We now need to load it into the environment. This involves
   * several linker-level steps like allocating executable memory, resolving
   * symbols and re-locations, and registering EH frames for exception handling.
   */

  auto object = llvm::object::ObjectFile::createObjectFile(
      object_code()->getMemBufferRef());
  if (auto error = object.takeError()) {
    LOG_ERROR("LLVMEngine: Error constructing object file '{}'",
              llvm::toString(std::move(error)));
    return;
  }

  llvm::RuntimeDyld loader(*memory_manager(), *memory_manager());

  loader.loadObject(*object.get());

  if (loader.hasError()) {
    // Error
    LOG_ERROR("LLVMEngine: Error loading object file {}",
              loader.getErrorString().str());
    return;
  }

  loader.resolveRelocations();

  loader.registerEHFrames();

  memory_manager()->finalizeMemory();

  /*
   * Now, the object has successfully been loaded and is executable. We pull out
   * all module functions into a handy cache.
   */

  for (const auto &func : module.functions()) {
    auto symbol = loader.getSymbol(func.name());
    functions_[func.name()] = reinterpret_cast<void *>(symbol.getAddress());
  }

  set_loaded(true);
}

// ---------------------------------------------------------
// LLVM Engine
// ---------------------------------------------------------

void LLVMEngine::Initialize() {
  // Global LLVM initialization
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  // Make all exported TPL symbols available to JITed code
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

void LLVMEngine::Shutdown() { llvm::llvm_shutdown(); }

std::unique_ptr<LLVMEngine::CompiledModule> LLVMEngine::Compile(
    const vm::BytecodeModule &module, const CompilerOptions &options) {
  CompiledModuleBuilder builder(options, module);

  builder.DeclareFunctions();

  builder.DefineFunctions();

  builder.RunSimpleOptimizations();

  builder.Verify();

  builder.RunAggressiveOptimizations();

  auto compiled_module = builder.Finalize();

  compiled_module->Load(module);

  return compiled_module;
}

}  // namespace tpl::vm