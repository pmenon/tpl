#include "parsing/token.h"

namespace tpl {

#define T(name, str, precedence) #name,
const char *Token::name_[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) str,
const char *Token::string_[] = {TOKENS(T, T)};
#undef T

#define T(name, str, precedence) precedence,
const uint32_t Token::precedence_[] = {TOKENS(T, T)};
#undef T

}  // namespace tpl