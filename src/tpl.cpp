#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>

#include "ast/pretty_print.h"
#include "parsing/parser.h"
#include "parsing/scanner.h"
#include "tpl.h"

namespace tpl {

static constexpr const char *kExitKeyword = ".exit";

static void RunRepl() {
  util::Region region("repl");

  while (true) {
    std::string input;

    std::string line;
    do {
      printf("> ");
      std::getline(std::cin, line);

      if (line == kExitKeyword) {
        std::cout << region.get_info() << std::endl;
        return;
      }

      input.append(line);
    } while (!line.empty());

    // Let's parse the source
    parsing::Scanner scanner(input.data(), input.length());
    ast::AstNodeFactory node_factory(region);
    ast::AstStringsContainer strings_container(region);
    parsing::Parser parser(scanner, node_factory, strings_container);

    // Parse!
    ast::AstNode *root = parser.Parse();

    // For now, just pretty print the AST
    ast::PrettyPrint pretty_print(root);
    pretty_print.Print();
  }
}

static void RunFile(const std::string &filename) {}

} // namespace tpl

int main(int argc, char **argv) {
  printf("Welcome to TPL (ver. %u.%u)\n", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  if (argc == 2) {
    std::string filename(argv[1]);
    tpl::RunFile(filename);
  } else if (argc == 1) {
    tpl::RunRepl();
  }

  return 0;
}
