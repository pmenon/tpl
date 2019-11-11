#include <unistd.h>
#include <algorithm>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "tbb/task_scheduler_init.h"

#include "llvm/Support/CommandLine.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/Path.h"

#include "ast/ast_dump.h"
#include "common/cpu_info.h"
#include "logging/logger.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"
#include "sema/sema.h"
#include "sql/catalog.h"
#include "sql/execution_context.h"
#include "sql/tablegen/table_generator.h"
#include "tpl.h"  // NOLINT
#include "util/timer.h"
#include "vm/bytecode_generator.h"
#include "vm/bytecode_module.h"
#include "vm/llvm_engine.h"
#include "vm/module.h"
#include "vm/vm.h"

// ---------------------------------------------------------
// CLI options
// ---------------------------------------------------------

// clang-format off
llvm::cl::OptionCategory kTplOptionsCategory("TPL Compiler Options","Options for controlling the TPL compilation process."); // NOLINT
llvm::cl::opt<bool> kPrintAst("print-ast", llvm::cl::desc("Print the programs AST"), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<bool> kPrintTbc("print-tbc", llvm::cl::desc("Print the generated TPL Bytecode"), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<bool> kIsSQL("sql", llvm::cl::desc("Is the input a SQL query?"), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<bool> kTpch("tpch", llvm::cl::desc("Should the TPCH database be loaded? Requires '-schema' and '-data' directories."), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<std::string> kDataDir("data", llvm::cl::desc("Where to find data files of tables to load"), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
llvm::cl::opt<std::string> kInputFile(llvm::cl::Positional, llvm::cl::desc("<input file>"), llvm::cl::init(""), llvm::cl::cat(kTplOptionsCategory));  // NOLINT
// clang-format on

tbb::task_scheduler_init scheduler;  // NOLINT

namespace tpl {

static constexpr const char *kExitKeyword = ".exit";

/**
 * Compile TPL source code contained in @em source and execute it in all execution modes once.
 * @param source The TPL source.
 * @param name The name of the TPL file.
 */
static void CompileAndRun(const std::string &source, const std::string &name = "tmp-tpl") {
  // Let's parse the source
  sema::ErrorReporter error_reporter;
  ast::Context context(&error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(&scanner, &context);

  sql::NoOpResultConsumer consumer;
  sql::tablegen::TPCHOutputSchemas schemas;
  const sql::Schema *schema = schemas.GetSchema(
      llvm::sys::path::filename(name).take_until([](char x) { return x == '.'; }));

  double parse_ms = 0.0,       // Time to parse the source
      typecheck_ms = 0.0,      // Time to perform semantic analysis
      codegen_ms = 0.0,        // Time to generate TBC
      interp_exec_ms = 0.0,    // Time to execute the program in fully interpreted mode
      adaptive_exec_ms = 0.0,  // Time to execute the program in adaptive mode
      jit_exec_ms = 0.0;       // Time to execute the program in JIT excluding compilation time

  //
  // Parse
  //

  ast::AstNode *root;
  {
    util::ScopedTimer<std::milli> timer(&parse_ms);
    root = parser.Parse();
  }

  if (error_reporter.HasErrors()) {
    LOG_ERROR("Parsing error!");
    error_reporter.PrintErrors(std::cerr);
    return;
  }

  //
  // Type check
  //

  {
    util::ScopedTimer<std::milli> timer(&typecheck_ms);
    sema::Sema type_check(&context);
    type_check.Run(root);
  }

  if (error_reporter.HasErrors()) {
    LOG_ERROR("Type-checking error!");
    error_reporter.PrintErrors(std::cerr);
    return;
  }

  // Dump AST
  if (kPrintAst) {
    ast::AstDump::Dump(root);
  }

  //
  // TBC generation
  //

  std::unique_ptr<vm::BytecodeModule> bytecode_module;
  {
    util::ScopedTimer<std::milli> timer(&codegen_ms);
    bytecode_module = vm::BytecodeGenerator::Compile(root, name);
  }

  // Dump Bytecode
  if (kPrintTbc) {
    bytecode_module->Dump(std::cout);
  }

  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  //
  // Interpret
  //

  {
    util::ScopedTimer<std::milli> timer(&interp_exec_ms);

    if (kIsSQL) {
      std::function<uint32_t(sql::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Interpret, main)) {
        LOG_ERROR("Missing 'main' entry function with signature (*ExecutionContext)->int32");
        return;
      }
      sql::MemoryPool memory(nullptr);
      sql::ExecutionContext exec_ctx(&memory, schema, &consumer);
      LOG_INFO("VM main() returned: {}", main(&exec_ctx));
    } else {
      std::function<uint32_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Interpret, main)) {
        LOG_ERROR("Missing 'main' entry function with signature ()->int32");
        return;
      }
      LOG_INFO("VM main() returned: {}", main());
    }
  }

  //
  // Adaptive
  //

  {
    util::ScopedTimer<std::milli> timer(&adaptive_exec_ms);

    if (kIsSQL) {
      std::function<uint32_t(sql::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Adaptive, main)) {
        LOG_ERROR("Missing 'main' entry function with signature (*ExecutionContext)->int32");
        return;
      }
      sql::MemoryPool memory(nullptr);
      sql::ExecutionContext exec_ctx(&memory, schema, &consumer);
      LOG_INFO("ADAPTIVE main() returned: {}", main(&exec_ctx));
    } else {
      std::function<uint32_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Adaptive, main)) {
        LOG_ERROR("Missing 'main' entry function with signature ()->int32");
        return;
      }
      LOG_INFO("ADAPTIVE main() returned: {}", main());
    }
  }

  //
  // JIT
  //
  {
    util::ScopedTimer<std::milli> timer(&jit_exec_ms);

    if (kIsSQL) {
      std::function<uint32_t(sql::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Compiled, main)) {
        LOG_ERROR("Missing 'main' entry function with signature (*ExecutionContext)->int32");
        return;
      }
      util::Timer<std::milli> x;
      x.Start();
      sql::MemoryPool memory(nullptr);
      sql::ExecutionContext exec_ctx(&memory, schema, &consumer);
      LOG_INFO("JIT main() returned: {}", main(&exec_ctx));
      x.Stop();
      LOG_INFO("Jit exec: {} ms", x.GetElapsed());
    } else {
      std::function<uint32_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Compiled, main)) {
        LOG_ERROR("Missing 'main' entry function with signature ()->int32");
        return;
      }
      LOG_INFO("JIT main() returned: {}", main());
    }
  }

  // Dump stats
  LOG_INFO(
      "Parse: {} ms, Type-check: {} ms, Code-gen: {} ms, Interp. Exec.: {} ms, "
      "Adaptive Exec.: {} ms, Jit+Exec.: {} ms",
      parse_ms, typecheck_ms, codegen_ms, interp_exec_ms, adaptive_exec_ms, jit_exec_ms);
}

/**
 * Compile and run the TPL program contained in the file with the given filename @em filename.
 * @param filename The name of TPL file to compile and run.
 */
static void RunFile(const std::string &filename) {
  auto file = llvm::MemoryBuffer::getFile(filename);
  if (std::error_code error = file.getError()) {
    LOG_ERROR("There was an error reading file '{}': {}", filename, error.message());
    return;
  }

  LOG_INFO("Compiling and running file: {}", filename);

  // Copy the source into a temporary, compile, and run
  CompileAndRun((*file)->getBuffer().str(), filename);
}

/**
 * Run the REPL.
 */
static void RunRepl() {
  const auto prompt_and_read_line = [] {
    std::string line;
    printf(">>> ");
    std::getline(std::cin, line);
    return line;
  };

  while (true) {
    std::string line = prompt_and_read_line();

    // Exit?
    if (line == kExitKeyword) {
      return;
    }

    // Run file?
    if (llvm::StringRef line_ref(line); line_ref.startswith_lower(".run")) {
      auto [_, filename] = line_ref.split(' ');
      (void)_;
      RunFile(filename);
      continue;
    }

    // Code ...
    std::string input = line;
    while (!line.empty()) {
      input.append(line).append("\n");
      line = prompt_and_read_line();
    }
    // Try to compile and run it
    CompileAndRun(input);
  }
}

/**
 * Initialize all TPL subsystems in preparation for execution.
 */
void InitTPL() {
  // Logging infra
  logging::InitLogger();

  // CPU info
  CpuInfo::Instance();

  // LLVM initialization
  vm::LLVMEngine::Initialize();

  // Catalog init
  tpl::sql::Catalog::Instance();

  LOG_INFO("TPL Bytecode Count: {}", tpl::vm::Bytecodes::NumBytecodes());

  LOG_INFO("TPL initialized ...");
}

/**
 * Shutdown all TPL subsystems.
 */
void ShutdownTPL() {
  tpl::vm::LLVMEngine::Shutdown();

  tpl::logging::ShutdownLogger();

  scheduler.terminate();

  LOG_INFO("TPL cleanly shutdown ...");
}

}  // namespace tpl

void SignalHandler(int32_t sig_num) {
  if (sig_num == SIGINT) {
    tpl::ShutdownTPL();
    exit(0);
  }
}

int main(int argc, char **argv) {
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

  if (kTpch) {
    if (kDataDir.empty()) {
      LOG_ERROR("Must specify '-data' directories when loading TPC-H");
      return -1;
    }

    tpl::sql::tablegen::TableGenerator::GenerateTPCHTables(tpl::sql::Catalog::Instance(), kDataDir);
  }

  LOG_INFO("\n{}", tpl::CpuInfo::Instance()->PrettyPrintInfo());

  LOG_INFO("Welcome to TPL (ver. {}.{})", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  // Either execute a TPL program from a source file, or run REPL
  if (!kInputFile.empty()) {
    tpl::RunFile(kInputFile);
  } else {
    tpl::RunRepl();
  }

  // Cleanup
  tpl::ShutdownTPL();

  return 0;
}
