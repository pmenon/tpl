#pragma once

#include "util/common.h"

namespace tpl::sql {

/// All possible JOIN types
enum class JoinType : u8 { Inner, Outer, Left, Right, Anti, Semi };

}  // namespace tpl::sql