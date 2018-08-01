#include <cstdio>
#include <iostream>
#include <string>

#include "scanner.h"
#include "tpl.h"

static void RunRepl() {
  tpl::Scanner scanner;

  for (;;) {
    std::string input;

    std::cout << "> ";
    std::getline(std::cin, input);

    if (input.empty()) {
      break;
    }

    scanner.Initialize(input.data(), input.length());

    for (auto token = scanner.Next(); token != tpl::Token::Type::EOS;
         token = scanner.Next()) {
      std::cout << tpl::Token::Name(token) << std::endl;
    }
  }
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
