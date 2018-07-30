#include <cstdio>
#include <string>
#include <scanner.h>

#include "tpl.h"

static void RunRepl() {

}

static void RunFile(const std::string &filename) {

}

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
