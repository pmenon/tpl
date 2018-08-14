#include "gtest/gtest.h"

#include <unordered_set>

#include "ast/ast.h"
#include "ast/ast_node_factory.h"

namespace tpl::ast::test {

TEST(AstStringsContainer, CreateNewStringsTest) {
  util::Region tmp_region("test");
  AstStringsContainer strings(tmp_region);

  // We request the strings "string-0", "string-1", ..., "string-99" from the
  // strings container. Since each string is unique, the string container should
  // never return a duplicate pointer to a string.

  std::unordered_set<const AstString *> generated;
  for (uint32_t i = 0; i < 100; i++) {
    AstString *string = strings.GetAstString("string-" + std::to_string(i));
    EXPECT_EQ(0u, generated.count(string));

    // Check all strings j < i. These must return previously acquired pointers
    for (uint32_t j = 0; j < i; j++) {
      AstString *dup_request =
          strings.GetAstString("string-" + std::to_string(j));
      EXPECT_EQ(1u, generated.count(dup_request));
    }

    generated.insert(string);
  }
}

}  // namespace tpl::ast::test