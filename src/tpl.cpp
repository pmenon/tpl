#include <unistd.h>
#include <algorithm>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>

#include "llvm/Support/CommandLine.h"
#include "llvm/Support/MemoryBuffer.h"

#include "ast/ast_dump.h"
#include "logging/logger.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"
#include "sema/sema.h"
#include "sql/execution_structures.h"
#include "tpl.h"  // NOLINT
#include "util/cpu_info.h"
#include "util/timer.h"
#include "vm/bytecode_generator.h"
#include "vm/bytecode_module.h"
#include "vm/llvm_engine.h"
#include "vm/vm.h"

// ---------------------------------------------------------
// CLI options
// ---------------------------------------------------------

// clang-format off
llvm::cl::OptionCategory kTplOptionsCategory("TPL Compiler Options", "Options for controlling the TPL compilation process.");  // NOLINT
llvm::cl::opt<std::string> kInputFile(llvm::cl::Positional, llvm::cl::desc("<input file>"), llvm::cl::init(""), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<bool> kPrintAst("print-ast", llvm::cl::desc("Print the programs AST"), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<bool> kPrintTbc("print-tbc", llvm::cl::desc("Print the generated TPL Bytecode"), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
// clang-format on

namespace tpl {

static constexpr const char *kExitKeyword = ".exit";

/// Compile the TPL source in \a source and run it in both interpreted and JIT
/// compiled mode
/// \param source The TPL source
/// \param name The name of the module/program
static void CompileAndRun(const std::string &source,
                          const std::string &name = "tmp-tpl") {
  util::Region region("repl-ast");
  util::Region error_region("repl-error");

  // Let's parse the source
  sema::ErrorReporter error_reporter(&error_region);
  ast::Context context(&region, &error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(&scanner, &context);

  double parse_ms = 0, typecheck_ms = 0, codegen_ms = 0, exec_ms = 0,
                                                            jit_ms = 0;

  // Parse
  ast::AstNode *root;
  {
    util::ScopedTimer<std::milli> timer(&parse_ms);
    root = parser.Parse();
  }

  if (error_reporter.HasErrors()) {
    LOG_ERROR("Parsing error!");
    error_reporter.PrintErrors();
    return;
  }

  // Type check
  {
    util::ScopedTimer<std::milli> timer(&typecheck_ms);
    sema::Sema type_check(&context);
    type_check.Run(root);
  }

  if (error_reporter.HasErrors()) {
    LOG_ERROR("Type-checking error!");
    error_reporter.PrintErrors();
    return;
  }

  // Dump AST
  if (kPrintAst) {
    ast::AstDump::Dump(root);
  }

  // Codegen
  std::unique_ptr<vm::BytecodeModule> module;
  {
    util::ScopedTimer<std::milli> timer(&codegen_ms);
    module = vm::BytecodeGenerator::Compile(root, name);
  }

  // Dump Bytecode
  if (kPrintTbc) {
    module->PrettyPrint(std::cout);
  }

  // Interpret
  {
    util::ScopedTimer<std::milli> timer(&exec_ms);

    std::function<u32()> main_func;
    if (!module->GetFunction("main", vm::ExecutionMode::Interpret, main_func)) {
      LOG_ERROR("No main() entry function found with signature ()->int32");
      return;
    }

    LOG_INFO("VM main() returned: {}", main_func());
  }

  // JIT
  {
    util::ScopedTimer<std::milli> timer(&jit_ms);

    std::function<u32()> main_func;
    if (!module->GetFunction("main", vm::ExecutionMode::Jit, main_func)) {
      LOG_ERROR("No main() entry function found with signature ()->int32");
      return;
    }

    LOG_INFO("JIT main() returned: {}", main_func());
  }

  // Dump stats
  LOG_INFO(
      "Parse: {} ms, Type-check: {} ms, Code-gen: {} ms, Exec.: {} ms, "
      "Jit+Exec.: {} ms",
      parse_ms, typecheck_ms, codegen_ms, exec_ms, jit_ms);
}

/// Run the TPL REPL
static void RunRepl() {
  while (true) {
    std::string input;

    std::string line;
    do {
      printf(">>> ");
      std::getline(std::cin, line);

      if (line == kExitKeyword) {
        return;
      }

      input.append(line).append("\n");
    } while (!line.empty());

    CompileAndRun(input);
  }
}

/// Compile and run the TPL program in the given filename
/// \param filename The name of the file on disk to compile
static void RunFile(const std::string &filename) {
  auto file = llvm::MemoryBuffer::getFile(filename);
  if (std::error_code error = file.getError()) {
    LOG_ERROR("There was an error reading file '{}': {}", filename,
              error.message());
    return;
  }

  LOG_INFO("Compiling and running file: {}", filename);

  // Copy the source into a temporary, compile, and run
  CompileAndRun((*file)->getBuffer().str());
}

/// Initialize all TPL subsystems
void InitTPL() {
  tpl::logging::InitLogger();

  tpl::CpuInfo::Instance();

  tpl::sql::ExecutionStructures::Instance();

  tpl::vm::LLVMEngine::Initialize();

  LOG_INFO("TPL Bytecode Count: {}", tpl::vm::Bytecodes::NumBytecodes());

  LOG_INFO("TPL initialized ...");
}

/// Shutdown all TPL subsystems
void ShutdownTPL() {
  tpl::vm::LLVMEngine::Shutdown();

  tpl::logging::ShutdownLogger();

  LOG_INFO("TPL cleanly shutdown ...");
}

}  // namespace tpl

void SignalHandler(i32 sig_num) {
  if (sig_num == SIGINT) {
    tpl::ShutdownTPL();
    exit(0);
  }
}

int main(int argc, char **argv) {  // NOLINT(bugprone-exception-escape)
  // Parse options
  llvm::cl::HideUnrelatedOptions(kTplOptionsCategory);
  llvm::cl::ParseCommandLineOptions(argc, argv);

  // Initialize a signal handler to call SignalHandler()
  struct sigaction sa;
  sa.sa_handler = &SignalHandler;
  sa.sa_flags = SA_RESTART;

  sigfillset(&sa.sa_mask);

  if (sigaction(SIGINT, &sa, nullptr) == -1) {
    LOG_ERROR("Cannot handle SIGNIT: {}", strerror(errno));
    return errno;
  }

  // Init TPL
  tpl::InitTPL();

  LOG_INFO("\n{}", tpl::CpuInfo::Instance()->PrettyPrintInfo());

  LOG_INFO("Welcome to TPL (ver. {}.{})", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  // Either execute a TPL program from a source file, or run REPL
  if (!kInputFile.empty()) {
    tpl::RunFile(kInputFile);
  } else if (argc == 1) {
    tpl::RunRepl();
  }

  // Cleanup
  tpl::ShutdownTPL();

  return 0;
}
