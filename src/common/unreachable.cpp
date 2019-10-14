#include "common/macros.h"

#include <cstdio>
#include <cstdlib>

#include "common/common.h"

namespace tpl {

void tpl_unreachable(const char *msg, const char *file, unsigned int line) {
  if (msg != nullptr) {
    fprintf(stderr, "%s\n", msg);
  }
  fprintf(stderr, "UNREACHABLE point reached");
  if (file != nullptr) {
    fprintf(stderr, " at %s:%u", file, line);
  }
  fprintf(stderr, "!\n");
  abort();
  __builtin_unreachable();
}

}  // namespace tpl
