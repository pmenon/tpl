#include <memory>

#include "ast/context.h"
#include "sema/error_reporter.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/compact_storage.h"
#include "sql/codegen/compilation_unit.h"
#include "sql/schema.h"

// Tests
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

class CompactStorageTest : public CodegenBasedTest {
 public:
  CompactStorageTest()
      : errors_(std::make_unique<sema::ErrorReporter>()),
        ctx_(std::make_unique<ast::Context>(errors_.get())),
        cu_(std::make_unique<CompilationUnit>(ctx_.get(), "test")),
        codegen_(std::make_unique<CodeGen>(cu_.get())) {}

 protected:
  std::unique_ptr<sema::ErrorReporter> errors_;
  std::unique_ptr<ast::Context> ctx_;
  std::unique_ptr<CompilationUnit> cu_;
  std::unique_ptr<CodeGen> codegen_;
};

TEST_F(CompactStorageTest, OrderingCheck) {
  static constexpr std::string_view kMemberName = "member";

  // Provide worst-ordering.
  CompactStorage storage(codegen_.get(), "Row",
                         {
                             TypeId::Integer,    // a
                             TypeId::SmallInt,   // b
                             TypeId::Varchar,    // c
                             TypeId::Double,     // d
                             TypeId::Date,       // e
                             TypeId::Varchar,    // f
                             TypeId::Timestamp,  // g
                         });

  // After finalizing, the order should be:
  // field: c, f, d, g, a, e, b
  //  slot: 0, 1, 2, 3, 4, 5, 6
  // ordered by descending size in stable order.

  EXPECT_EQ(fmt::format("{}4", kMemberName), storage.FieldNameAtIndex(0).ToString());
  EXPECT_EQ(fmt::format("{}6", kMemberName), storage.FieldNameAtIndex(1).ToString());
  EXPECT_EQ(fmt::format("{}0", kMemberName), storage.FieldNameAtIndex(2).ToString());
  EXPECT_EQ(fmt::format("{}2", kMemberName), storage.FieldNameAtIndex(3).ToString());
  EXPECT_EQ(fmt::format("{}5", kMemberName), storage.FieldNameAtIndex(4).ToString());
  EXPECT_EQ(fmt::format("{}1", kMemberName), storage.FieldNameAtIndex(5).ToString());
  EXPECT_EQ(fmt::format("{}3", kMemberName), storage.FieldNameAtIndex(6).ToString());
}

}  // namespace tpl::sql::codegen
