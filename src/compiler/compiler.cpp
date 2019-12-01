#include "compiler/compiler.h"

#include "ast/context.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"
#include "sema/sema.h"
#include "vm/bytecode_generator.h"
#include "vm/module.h"

namespace tpl::compiler {

//===----------------------------------------------------------------------===//
//
// Compiler Input
//
//===----------------------------------------------------------------------===//

Compiler::Input::Input(const std::string &name, ast::Context *context, const std::string &source)
    : name_(name), context_(context), root_(nullptr), source_(&source) {}

Compiler::Input::Input(const std::string &name, ast::Context *context, ast::AstNode *root)
    : name_(name), context_(context), root_(root), source_(nullptr) {}

//===----------------------------------------------------------------------===//
//
// Compiler
//
//===----------------------------------------------------------------------===//

Compiler::Compiler(const Compiler::Input &input)
    : input_(input), root_(input.IsFromAST() ? input.root_ : nullptr), bytecode_module_(nullptr) {
  // Reset errors
  GetErrorReporter()->Reset();
}

// Required because we forward-declared the classes we use as templates to unique_ptr<> members.
Compiler::~Compiler() = default;

sema::ErrorReporter *Compiler::GetErrorReporter() const { return GetContext()->error_reporter(); }

bool Compiler::Parse(Compiler::Callbacks *callbacks) {
  if (root_ != nullptr) {
    // Input provided AST, no need to parse
    TPL_ASSERT(input_.IsFromAST(), "No source should exist when compiling directly from AST");
    return true;
  }

  TPL_ASSERT(input_.IsFromSource(),
             "No AST should exist before parsing when compiling from source");

  // The input is TPL source, we need to parse it.
  if (callbacks->OnBeginParse(this) == Response::Abort) {
    return false;
  }

  // Setup the scanner and parse
  parsing::Scanner scanner(*input_.source_);
  parsing::Parser parser(&scanner, GetContext());
  root_ = parser.Parse();

  // Errors?
  if (root_ == nullptr || GetErrorReporter()->HasErrors()) {
    callbacks->OnError(this);
    return false;
  }

  // Continue?
  if (callbacks->OnEndParse(this) == Response::Abort) {
    return false;
  }

  return true;
}

bool Compiler::SemanticAnalysis(Compiler::Callbacks *callbacks) {
  TPL_ASSERT(root_ != nullptr, "Must have AST before reaching semantic analysis");

  // Continue?
  if (callbacks->OnBeginSemanticAnalysis(this) == Response::Abort) {
    return false;
  }

  sema::Sema semantic_analysis(GetContext());
  semantic_analysis.Run(root_);

  // Errors?
  if (GetErrorReporter()->HasErrors()) {
    callbacks->OnError(this);
    return false;
  }

  // Continue?
  if (callbacks->OnEndSemanticAnalysis(this) == Response::Abort) {
    return false;
  }

  return true;
}

std::unique_ptr<vm::BytecodeModule> Compiler::GenerateBytecode(Compiler::Callbacks *callbacks) {
  // Continue?
  if (callbacks->OnBeginBytecodeGeneration(this) == Response::Abort) {
    return nullptr;
  }

  auto bytecode_module = vm::BytecodeGenerator::Compile(root_, "name_");
  bytecode_module_ = bytecode_module.get();

  // Errors?
  if (GetErrorReporter()->HasErrors()) {
    callbacks->OnError(this);
    return nullptr;
  }

  // Continue?
  if (callbacks->OnEndBytecodeGeneration(this) == Response::Abort) {
    return nullptr;
  }

  return bytecode_module;
}

void Compiler::GenerateModule(std::unique_ptr<vm::BytecodeModule> bytecode_module,
                              Compiler::Callbacks *callbacks) {
  // Continue?
  if (callbacks->OnBeginModuleGeneration(this) == Response::Abort) {
    return;
  }

  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  // Errors?
  if (GetErrorReporter()->HasErrors()) {
    callbacks->OnError(this);
    return;
  }

  // Continue?
  if (callbacks->OnEndModuleGeneration(this) == Response::Abort) {
    return;
  }

  // Done
  callbacks->TakeOwnership(std::move(module));
}

void Compiler::Run(Compiler::Callbacks *callbacks) {
  bool valid;

  valid = Parse(callbacks);

  if (!valid) {
    return;
  }

  valid = SemanticAnalysis(callbacks);

  if (!valid) {
    return;
  }

  auto bytecode_module = GenerateBytecode(callbacks);
  valid = bytecode_module != nullptr;

  if (!valid) {
    return;
  }

  GenerateModule(std::move(bytecode_module), callbacks);
}

void Compiler::RunCompilation(const Compiler::Input &input, Compiler::Callbacks *callbacks) {
  TPL_ASSERT(callbacks != nullptr, "Must provide callbacks");
  Compiler compiler(input);
  compiler.Run(callbacks);
}

TimePasses::TimePasses() : parse_ms_(0), sema_ms_(0), tbc_gen_ms_(0), module_gen_ms_(0) {}

Compiler::Response TimePasses::OnBeginParse(Compiler *compiler) {
  timer_.Start();
  return Callbacks::OnBeginParse(compiler);
}

Compiler::Response TimePasses::OnEndParse(Compiler *compiler) {
  timer_.Stop();
  parse_ms_ = timer_.GetElapsed();
  return Callbacks::OnEndParse(compiler);
}

Compiler::Response TimePasses::OnBeginSemanticAnalysis(Compiler *compiler) {
  timer_.Start();
  return Callbacks::OnBeginSemanticAnalysis(compiler);
}

Compiler::Response TimePasses::OnEndSemanticAnalysis(Compiler *compiler) {
  timer_.Stop();
  sema_ms_ = timer_.GetElapsed();
  return Callbacks::OnEndSemanticAnalysis(compiler);
}

Compiler::Response TimePasses::OnBeginBytecodeGeneration(Compiler *compiler) {
  timer_.Start();
  return Callbacks::OnBeginBytecodeGeneration(compiler);
}

Compiler::Response TimePasses::OnEndBytecodeGeneration(Compiler *compiler) {
  timer_.Stop();
  tbc_gen_ms_ = timer_.GetElapsed();
  return Callbacks::OnEndBytecodeGeneration(compiler);
}

Compiler::Response TimePasses::OnBeginModuleGeneration(Compiler *compiler) {
  timer_.Start();
  return Callbacks::OnBeginModuleGeneration(compiler);
}

Compiler::Response TimePasses::OnEndModuleGeneration(Compiler *compiler) {
  timer_.Stop();
  module_gen_ms_ = timer_.GetElapsed();
  return Callbacks::OnEndModuleGeneration(compiler);
}

}  // namespace tpl::compiler
