#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>

#include "ast/ast_dump.h"
#include "logging/logger.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"
#include "sema/sema.h"
#include "sql/catalog.h"
#include "tpl.h"
#include "util/timer.h"
#include "vm/bytecode_generator.h"
#include "vm/module.h"
#include "vm/vm.h"

namespace tpl {

static constexpr const char *kExitKeyword = ".exit";

static void CompileAndRun(const std::string &source) {
  util::Region region("repl-ast");
  util::Region error_region("repl-error");

  // Let's parse the source
  sema::ErrorReporter error_reporter(&error_region);
  ast::AstContext context(&region, error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(scanner, context);

  double parse_ms = 0, typecheck_ms = 0, codegen_ms = 0, exec_ms = 0;

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
    sema::Sema type_check(context);
    type_check.Run(root);
  }

  if (error_reporter.HasErrors()) {
    LOG_ERROR("Type-checking error!");
    error_reporter.PrintErrors();
    return;
  }

  // Dump AST
  ast::AstDump::Dump(root);

  // Codegen
  std::unique_ptr<vm::Module> module;
  {
    util::ScopedTimer<std::milli> timer(&codegen_ms);
    module = vm::BytecodeGenerator::Compile(&region, root);
  }

  // Dump VM
  module->PrettyPrint(std::cout);

  // Execute
  {
    util::ScopedTimer<std::milli> timer(&exec_ms);
    vm::VM::Execute(&region, *module, "main");
  }

  // Dump stats
  LOG_INFO("Parse: {} ms, Typecheck: {} ms, Codegen: {} ms, Exec.: {} ms",
           parse_ms, typecheck_ms, codegen_ms, exec_ms);
}

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

static void RunFile(const std::string &filename) {
  std::string source;

  std::ifstream file(filename);
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      source.append(line).append("\n");
    }
    file.close();
  }

  CompileAndRun(source);
}

}  // namespace tpl

int main(int argc, char **argv) {
  // Init logging
  tpl::logging::init_logger();

  // Welcome
  LOG_INFO("Welcome to TPL (ver. {}.{})", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  // Init catalog
  tpl::sql::Catalog::instance();

  // Either execute a TPL program from a source file, or run REPL
  if (argc == 2) {
    std::string filename(argv[1]);
    tpl::RunFile(filename);
  } else if (argc == 1) {
    tpl::RunRepl();
  }

  return 0;
}
