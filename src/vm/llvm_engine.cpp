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
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

#include "ast/type.h"
#include "logging/logger.h"
#include "vm/bytecode_module.h"

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

  //  auto ret = builder.PrettyPrintLLVMModule();

  //  LOG_ERROR("{}", ret);

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
    auto error = module.takeError();
    LOG_ERROR("There was an error parsing the bitcode file!");
  }

  llvm_module_ = std::move(module.get());
}

void LLVMEngine::CompilationUnitBuilder::DeclareFunctions() {
  // Create function prototypes for all functions defined in the bytecode module
  llvm::Type *void_type = llvm::Type::getVoidTy(module()->getContext());

  for (const auto &func_info : tpl_module()->functions()) {
    llvm::SmallVector<llvm::Type *, 8> param_types;
    for (u32 i = 0; i < func_info.num_params(); i++) {
      const auto *tpl_type = func_info.locals()[i].type();
      param_types.emplace_back(GetLLVMType(tpl_type));
    }
    auto func_type = llvm::FunctionType::get(void_type, param_types, false);
    module()->getOrInsertFunction(func_info.name(), func_type);
  }
}

void LLVMEngine::CompilationUnitBuilder::DefineFunctions() {
  llvm::IRBuilder<> ir_builder(context());

  for (const auto &func_info : tpl_module()->functions()) {
    llvm::Function *func = module()->getFunction(func_info.name());
    TPL_ASSERT(func != nullptr,
               "All functions have to be declared before we define them");

    //auto *instructions = tpl_module()->GetBytecodeForFunction(func_info);
  }
}

void LLVMEngine::CompilationUnitBuilder::Verify() {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  if (bool valid = llvm::verifyModule(*module(), &ostream); !valid) {
    // TODO(pmenon): Do something more here ...
    LOG_ERROR("ERROR IN MODULE: {}", result);
  }
}

void LLVMEngine::CompilationUnitBuilder::Clean() {}

void LLVMEngine::CompilationUnitBuilder::Optimize() {
  llvm::legacy::FunctionPassManager pass_manager(module());
  pass_manager.add(llvm::createInstructionCombiningPass());
  pass_manager.add(llvm::createReassociatePass());
  pass_manager.add(llvm::createGVNPass());
  pass_manager.add(llvm::createCFGSimplificationPass());
  pass_manager.add(llvm::createAggressiveDCEPass());
  pass_manager.add(llvm::createCFGSimplificationPass());

  pass_manager.doInitialization();
  for (auto &func : *module()) {
    pass_manager.run(func);
  }
  pass_manager.doFinalization();
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

llvm::Type *LLVMEngine::CompilationUnitBuilder::GetLLVMType(
    const ast::Type *type) {
  // Check cache
  if (auto iter = type_map_.find(type); iter != type_map_.end()) {
    return iter->second;
  }

  llvm::LLVMContext &ctx = module()->getContext();
  llvm::Type *llvm_type = nullptr;

  switch (type->kind()) {
    case ast::Type::Kind::BoolType: {
      llvm_type = llvm::Type::getInt1Ty(ctx);
      break;
    }
    case ast::Type::Kind::IntegerType: {
      auto *int_type = type->As<ast::IntegerType>();
      llvm_type = llvm::Type::getIntNTy(ctx, int_type->BitWidth());
      break;
    }
    case ast::Type::Kind::FloatType: {
      auto *float_type = type->As<ast::FloatType>();
      if (float_type->float_kind() == ast::FloatType::FloatKind::Float32) {
        llvm_type = llvm::Type::getFloatTy(ctx);
      } else {
        llvm_type = llvm::Type::getDoubleTy(ctx);
      }
      break;
    }
    case ast::Type::Kind::NilType: {
      llvm_type = llvm::Type::getVoidTy(ctx);
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
      // TODO: me
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
      // TODO: me
      break;
    }
  }

  TPL_ASSERT(llvm_type != nullptr, "No LLVM type found!");

  // Cache
  type_map_.insert(std::make_pair(type, llvm_type));

  return llvm_type;
}

}  // namespace tpl::vm