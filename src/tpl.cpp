#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>

#include "ast/pretty_print.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "tpl.h"

static constexpr const char *kExitKeyword = ".exit";

static void RunRepl() {
  tpl::Region region("repl");

  while (true) {
    std::string input;

    printf("> ");
    std::getline(std::cin, input);

    if (input.empty() || input == kExitKeyword) {
      break;
    }

    // Let's parse the source
    tpl::Scanner scanner(input.data(), input.length());
    tpl::AstNodeFactory node_factory(region);
    tpl::AstStringsContainer strings_container(region);
    tpl::Parser parser(scanner, node_factory, strings_container);

    // Parse!
    tpl::AstNode *root = parser.Parse();

    // For now, just pretty print the AST
    tpl::PrettyPrint pretty_print(root);
    pretty_print.Print();
  }

  std::cout << region.get_info() << std::endl;
}

static void RunFile(const std::string &filename) {}

int main(int argc, char **argv) {
  printf("Welcome to TPL (ver. %u.%u)\n", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  if (argc == 2) {
    std::string filename(argv[1]);
    RunFile(filename);
  } else if (argc == 1) {
    RunRepl();
  }

  return 0;
}
