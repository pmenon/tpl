#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>

#include "parsing/scanner.h"
#include "tpl.h"

static void RunRepl() {
  for (;;) {
    std::string input;

    printf("> ");
    std::getline(std::cin, input);

    if (input.empty()) {
      break;
    }

    tpl::Scanner scanner(input.data(), input.length());

    for (auto token = scanner.Next(); token.type != tpl::Token::Type::EOS;
         token = scanner.Next()) {
      printf("[%lu, %lu]: %s %s\n", token.pos.line, token.pos.column,
             tpl::Token::Name(token.type), token.literal.c_str());
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
