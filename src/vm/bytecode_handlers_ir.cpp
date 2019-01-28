#include "vm/bytecode_handlers.h"

#include "vm/bytecodes.h"

extern "C" {

void *kAllFuncs[] = {
#define ENTRY(Name, ...) reinterpret_cast<void *>(&Op##Name),
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};
}
