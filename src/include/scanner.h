#pragma once

#include "token.h"

namespace tpl {

class Scanner {
 public:
  explicit Scanner(const char *source);

  Token Next();

  Token Peek();

 private:
  const char *source_;
};

}  // namespace tpl