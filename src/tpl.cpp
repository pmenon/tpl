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
#include "tpl.h"
#include "vm/bytecode_generator.h"
#include "vm/bytecode_unit.h"
#include "vm/vm.h"

namespace tpl {

static constexpr const char *kExitKeyword = ".exit";

static void Compile(const std::string &source) {
  util::Region region("repl-ast");
  util::Region error_region("repl-error");

  // Let's parse the source
  sema::ErrorReporter error_reporter(error_region);
  ast::AstContext context(region, error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(scanner, context);

  // Parsing
  ast::AstNode *root = parser.Parse();

  if (error_reporter.has_errors()) {
    LOG_ERROR("Parsing error!");
    error_reporter.PrintErrors();
    return;
  }

  // Type check
  sema::Sema type_check(context);
  if (type_check.Run(root)) {
    LOG_ERROR("Type-checking error!");
    error_reporter.PrintErrors();
    return;
  }

  // For now, just pretty print the AST
  ast::AstDump::Dump(root);

  // Try generating bytecode for this declaration
  auto unit = vm::BytecodeGenerator::Compile(root);

  unit->PrettyPrint(std::cout);
  vm::VM::Execute(*unit, "f");
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

      input.append(line);
    } while (!line.empty());

    Compile(input);
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

  Compile(source);
}

}  // namespace tpl

int main(int argc, char **argv) {
  tpl::logging::init_logger();

  LOG_INFO("Welcome to TPL (ver. {}.{})\n", TPL_VERSION_MAJOR,
           TPL_VERSION_MINOR);

  if (argc == 2) {
    std::string filename(argv[1]);
    tpl::RunFile(filename);
  } else if (argc == 1) {
    tpl::RunRepl();
  }

  return 0;
}
