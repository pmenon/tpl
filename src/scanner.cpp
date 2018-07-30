#include "scanner.h"

namespace tpl {

Scanner::Scanner(const char *source) : source_(source) {}

Token Scanner::Next() { return Token(); }
Token Scanner::Peek() { return Token(); }

}  // namespace tpl
