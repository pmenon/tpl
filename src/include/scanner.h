#pragma once

#include "token.h"

namespace tpl {

class Scanner {
 public:
  Scanner(const char *source);

  Token Next();

  Token Peek();

 private:
  const char *source;
};

}  // namespace tpl