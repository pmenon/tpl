#include "vm/llvm_engine.h"

#include "llvm/Bitcode/BitcodeReader.h"
#include "llvm/ExecutionEngine/MCJIT.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/ManagedStatic.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/IPO/AlwaysInliner.h"
#include "llvm/Transforms/IPO/Inliner.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

#include "ast/type.h"
#include "logging/logger.h"
#include "vm/bytecode_module.h"
#include "vm/bytecode_traits.h"

namespace tpl::vm {

// ---------------------------------------------------------
// LLVM Engine
// ---------------------------------------------------------

void LLVMEngine::Initialize() {
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
}

void LLVMEngine::Shutdown() { llvm::llvm_shutdown(); }

std::unique_ptr<LLVMEngine::CompilationUnit> LLVMEngine::Compile(
    vm::BytecodeModule *module) {
  CompileOptions options;

  CompilationUnitBuilder builder(options, module);

  builder.DeclareFunctions();

  builder.DefineFunctions();

  builder.Clean();

  builder.Verify();

  builder.Optimize();

  return builder.Finalize();
}

// ---------------------------------------------------------
// Compile Options
// ---------------------------------------------------------

std::string LLVMEngine::CompileOptions::GetBytecodeHandlersBcPath() const {
  return "./lib/bytecode_handlers.bc";
}

// ---------------------------------------------------------
// Compilation Unit
// ---------------------------------------------------------

LLVMEngine::CompilationUnit::CompilationUnit(llvm::Module *module)
    : module_(module) {}

void *LLVMEngine::CompilationUnit::GetFunctionPointer(
    const std::string &name) const {
  return nullptr;
}

// ---------------------------------------------------------
// Compilation Unit Builder
// ---------------------------------------------------------

LLVMEngine::CompilationUnitBuilder::CompilationUnitBuilder(
    const CompileOptions &options, vm::BytecodeModule *tpl_module)
    : tpl_module_(tpl_module), context_(std::make_unique<llvm::LLVMContext>()) {
  auto memory_buffer =
      llvm::MemoryBuffer::getFile(options.GetBytecodeHandlersBcPath());
  if (auto error = memory_buffer.getError()) {
    LOG_ERROR("There was an error loading the handler bytecode: {}",
              error.message());
  }

  auto module = llvm::parseBitcodeFile(*(memory_buffer.get()), context());
  if (!module) {
    llvm::SmallString<256> error;
    llvm::raw_svector_ostream os(error);
    os << module.takeError();
    LOG_ERROR("{}", error.c_str());
    throw std::runtime_error(error.str());
  }

  llvm_module_ = std::move(module.get());
  type_map_ = std::make_unique<TPLTypeToLLVMTypeMap>(llvm_module_.get());
}

void LLVMEngine::CompilationUnitBuilder::DeclareFunctions() {
  // Create a LLVM function declaration for each TPL function
  for (const auto &func_info : tpl_module()->functions()) {
    llvm::SmallVector<llvm::Type *, 8> param_types;

    for (u32 i = 0; i < func_info.num_params(); i++) {
      const auto *tpl_type = func_info.locals()[i].type();
      param_types.push_back(type_map()->GetLLVMType(tpl_type));
    }

    auto *func_type =
        llvm::FunctionType::get(type_map()->VoidType(), param_types, false);
    module()->getOrInsertFunction(func_info.name(), func_type);
  }
}

llvm::Function *LLVMEngine::CompilationUnitBuilder::LookupBytecodeHandler(
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

void LLVMEngine::CompilationUnitBuilder::DefineFunction(
    const FunctionInfo &func_info,
    llvm::IRBuilder<llvm::ConstantFolder, llvm::IRBuilderDefaultInserter>
        &ir_builder) {
  llvm::LLVMContext &ctx = ir_builder.getContext();
  llvm::Function *func = module()->getFunction(func_info.name());
  llvm::BasicBlock *entry = llvm::BasicBlock::Create(ctx, "EntryBB", func);

  ir_builder.SetInsertPoint(entry);

  /*
   * We first need to pre-process the function's body to discover all the basic
   * blocks. We store what we find in the blocks map below, sorted by the
   * bytecode positions.
   */

  std::vector<std::size_t> bb_begin_positions = {0};
  std::map<std::size_t, llvm::BasicBlock *> blocks = {{0, entry}};

  for (auto iter = tpl_module()->BytecodeForFunction(func_info);
       !bb_begin_positions.empty();) {
    std::size_t begin_pos = bb_begin_positions.back();
    bb_begin_positions.pop_back();

    for (iter.SetOffset(begin_pos); !iter.Done(); iter.Advance()) {
      Bytecode bytecode = iter.CurrentBytecode();

      bool is_terminal = Bytecodes::IsTerminal(bytecode);
      bool is_jump = Bytecodes::IsJump(bytecode);

      if (is_terminal && !is_jump) {
        break;
      }

      if (is_terminal) {
        std::size_t target_bb_pos =
            iter.current_offset() + iter.GetJumpOffsetOperand(0);
        blocks[target_bb_pos] = nullptr;
        break;
      }

      if (Bytecodes::IsJump(bytecode)) {
        std::size_t fallthrough_bb_pos =
            iter.current_offset() + iter.CurrentBytecodeSize();
        std::size_t branch_target_bb_pos =
            iter.current_offset() +
            Bytecodes::GetNthOperandOffset(bytecode, 1) +
            iter.GetJumpOffsetOperand(1);

        bb_begin_positions.push_back(branch_target_bb_pos);

        blocks[fallthrough_bb_pos] = nullptr;
        blocks[branch_target_bb_pos] = nullptr;
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
  LOG_INFO("Found blocks:");
  for (auto &[pos, block] : blocks) {
    LOG_INFO("  Block {} @ {:x}", block->getName().str(), pos);
  }
#endif

  /*
   * Now, we can define the function
   */

  auto block_iter = blocks.cbegin();

  LLVMFunctionHelper function_helper(func_info, func, type_map(), ir_builder);

  // Start construction of this function
  for (auto iter = tpl_module()->BytecodeForFunction(func_info); !iter.Done();
       iter.Advance()) {
    Bytecode bytecode = iter.CurrentBytecode();

    if (iter.current_offset() == block_iter->first) {
      ir_builder.SetInsertPoint(block_iter->second);
      ++block_iter;
    }

    // Get handler function
    llvm::Function *handler = LookupBytecodeHandler(bytecode);

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
          args.push_back(
              llvm::ConstantInt::get(llvm::Type::getInt16Ty(context()),
                                     iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::Imm4: {
          args.push_back(
              llvm::ConstantInt::get(llvm::Type::getInt32Ty(context()),
                                     iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::Imm8: {
          args.push_back(
              llvm::ConstantInt::get(llvm::Type::getInt64Ty(context()),
                                     iter.GetImmediateOperand(i), true));
          break;
        }
        case OperandType::UImm2: {
          args.push_back(llvm::ConstantInt::get(
              llvm::Type::getInt16Ty(context()),
              iter.GetUnsignedImmediateOperand(i), false));
          break;
        }
        case OperandType::JumpOffset: {
          break;
        }
        case OperandType::UImm4: {
          args.push_back(llvm::ConstantInt::get(
              llvm::Type::getInt32Ty(context()),
              iter.GetUnsignedImmediateOperand(i), false));
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

    // Call
    ir_builder.CreateCall(handler, args);
  }

  // Done
  ir_builder.CreateRetVoid();

  func->print(llvm::errs(), nullptr);
}

void LLVMEngine::CompilationUnitBuilder::DefineFunctions() {
  llvm::IRBuilder<> ir_builder(context());
  for (const auto &func_info : tpl_module()->functions()) {
    DefineFunction(func_info, ir_builder);
  }
}

void LLVMEngine::CompilationUnitBuilder::Verify() {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  if (bool has_error = llvm::verifyModule(*module(), &ostream); has_error) {
    // TODO(pmenon): Do something more here ...
    LOG_ERROR("ERROR IN MODULE: {}", result);
  }
}

void LLVMEngine::CompilationUnitBuilder::Clean() {
  llvm::legacy::PassManager pass_manager;
  pass_manager.add(llvm::createGlobalDCEPass());
  pass_manager.add(llvm::createAlwaysInlinerLegacyPass());
  pass_manager.run(*module());
}

void LLVMEngine::CompilationUnitBuilder::Optimize() {
  llvm::legacy::PassManager pass_manager;
  pass_manager.add(llvm::createInstructionCombiningPass());
  pass_manager.add(llvm::createReassociatePass());
  pass_manager.add(llvm::createGVNPass());
  pass_manager.add(llvm::createCFGSimplificationPass());
  pass_manager.add(llvm::createAggressiveDCEPass());
  pass_manager.add(llvm::createCFGSimplificationPass());
  pass_manager.run(*module());
}

std::unique_ptr<LLVMEngine::CompilationUnit>
LLVMEngine::CompilationUnitBuilder::Finalize() {
  auto *module_handle = module();

  std::string error_string;
  std::unique_ptr<llvm::ExecutionEngine> engine =
      std::unique_ptr<llvm::ExecutionEngine>(
          llvm::EngineBuilder(std::move(llvm_module_))
              .setEngineKind(llvm::EngineKind::JIT)
              .setMCPU(llvm::sys::getHostCPUName())
              .setErrorStr(&error_string)
              .create());

  // JIT compile the module
  engine->finalizeObject();

  // Done
  return std::make_unique<CompilationUnit>(module_handle);
}

std::string LLVMEngine::CompilationUnitBuilder::PrettyPrintLLVMModule() const {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  module()->print(ostream, nullptr);
  return result;
}

// ---------------------------------------------------------
// LLVM Function Helper
// ---------------------------------------------------------

LLVMEngine::LLVMFunctionHelper::LLVMFunctionHelper(
    const FunctionInfo &func_info, llvm::Function *func,
    TPLTypeToLLVMTypeMap *type_map, llvm::IRBuilder<> &ir_builder)
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

llvm::Value *LLVMEngine::LLVMFunctionHelper::GetArgumentById(LocalVar var) {
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
// TPL To LLVM Type Map
// ---------------------------------------------------------

LLVMEngine::TPLTypeToLLVMTypeMap::TPLTypeToLLVMTypeMap(llvm::Module *module)
    : module_(module) {
  llvm::LLVMContext &ctx = module->getContext();
  type_map_["nil"] = llvm::Type::getVoidTy(ctx);
  type_map_["bool"] = llvm::Type::getInt8Ty(ctx);
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

llvm::Type *LLVMEngine::TPLTypeToLLVMTypeMap::GetLLVMType(
    const ast::Type *type) {
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

}  // namespace tpl::vm