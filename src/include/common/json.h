#pragma once

#include "nlohmann_json.hpp"

namespace terrier::common {
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 */
using json = nlohmann::json;
}  // namespace terrier::common
