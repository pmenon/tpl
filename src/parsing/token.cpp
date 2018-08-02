#include "parsing/token.h"

namespace tpl {

#define T(name, str) #name,
const char *Token::name_[] = {TOKENS(T, T)};
#undef T

#define T(name, str) str,
const char *Token::string_[] = {TOKENS(T, T)};
#undef T

}  // namespace tpl