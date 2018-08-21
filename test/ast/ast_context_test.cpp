#include "gtest/gtest.h"

#include <unordered_set>

#include "ast/ast_context.h"
#include "ast/ast_node_factory.h"
#include "sema/error_reporter.h"

namespace tpl::ast::test {

TEST(AstStringsContainer, CreateNewStringsTest) {
  util::Region tmp_region("test");
  ast::AstNodeFactory node_factory(tmp_region);
  sema::ErrorReporter error_reporter(tmp_region);
  AstContext ctx(tmp_region, node_factory, error_reporter);

  // We request the strings "string-0", "string-1", ..., "string-99" from the
  // context. We expect duplicate input strings to return the same Identifier!

  std::unordered_set<Identifier, IdentifierHasher, IdentifierEquality> seen;
  for (uint32_t i = 0; i < 100; i++) {
    auto string = ctx.GetIdentifier("string-" + std::to_string(i));
    EXPECT_EQ(0u, seen.count(string));

    // Check all strings j < i. These must return previously acquired pointers
    for (uint32_t j = 0; j < i; j++) {
      auto dup_request = ctx.GetIdentifier("string-" + std::to_string(j));
      EXPECT_EQ(1u, seen.count(dup_request));
    }

    seen.insert(string);
  }
}

}  // namespace tpl::ast::test