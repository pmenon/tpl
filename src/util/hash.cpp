#include "util/hash.h"

#include "xxh3.h"  // NOLINT

namespace tpl::util {

hash_t Hasher::HashXX3(const uint8_t *buf, const uint32_t len, const hash_t seed) {
  return XXH3_64bits_withSeed(buf, len, seed);
}

}  // namespace tpl::util
