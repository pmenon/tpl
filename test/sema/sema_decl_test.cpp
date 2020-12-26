#include <vector>

#include "ast/ast_builder.h"
#include "util/test_harness.h"

#include "sema/sema.h"

namespace tpl::sema {

class SemaDeclTest : public TplTest, public ast::TestAstBuilder {
 public:
  void ResetErrorReporter() { error_reporter()->Reset(); }
};

struct TestCase {
  bool has_errors;
  std::string msg;
  ast::AstNode *tree;
};

TEST_F(SemaDeclTest, DuplicateStructFields) {
  TestCase tests[] = {
      // Test case 1 should fail.
      {true, "Struct has duplicate 'a' fields",
       GenFile({
           // The single struct declaration in the file.
           DeclStruct(Ident("s"),
                      {
                          GenFieldDecl(Ident("a"), PrimIntTypeRepr()),  // a: int
                          GenFieldDecl(Ident("b"), PrimIntTypeRepr()),  // b: int
                          GenFieldDecl(Ident("a"), PrimIntTypeRepr()),  // a: int
                      }),
       })},

      // Test case 2 should fail.
      {true, "Struct has duplicate 'a' with different types",
       GenFile({
           // The single struct declaration in the file.
           DeclStruct(Ident("s"),
                      {
                          GenFieldDecl(Ident("a"), PrimIntTypeRepr()),            // a: int
                          GenFieldDecl(Ident("a"), PtrType(PrimBoolTypeRepr())),  // a: *bool
                      }),
       })},

      // Test case 3 should be fine.
      {false, "Struct has only unique fields and should pass type-checking",
       GenFile({
           // The single struct declaration in the file.
           DeclStruct(Ident("s"),
                      {
                          GenFieldDecl(Ident("c"), PrimIntTypeRepr()),  // c: int
                          GenFieldDecl(Ident("d"), PrimIntTypeRepr()),  // d: int
                      }),
       })},
  };

  for (const auto &test : tests) {
    Sema sema(ctx());
    bool has_errors = sema.Run(test.tree);
    EXPECT_EQ(test.has_errors, has_errors) << test.msg;
    ResetErrorReporter();
  }
}

TEST_F(SemaDeclTest, AnonymousStructFields) {
  // What we're creating:
  // struct s {
  //   a: struct {
  //     x: int
  //   }
  //   b: struct {
  //     x: int
  //     y: int
  //   }
  //   c: struct {
  //     d: struct {
  //       x: int
  //       y: int
  //     }
  //   }
  // }
  //
  // Checks:
  // 1. 's' is a named struct.
  // 2. 's.a', 's.b', 's.c', and 's.c.d' are anonymous struct types.
  // 3. The type pointers for 's.b' and 's.c.d' must be the same due to caching.

  auto root = GenFile({
      DeclStruct(
          Ident("s"),
          {
              GenFieldDecl(Ident("a"), StructRepr({GenFieldDecl(Ident("x"), PrimIntTypeRepr())})),
              GenFieldDecl(Ident("b"), StructRepr({
                                           GenFieldDecl(Ident("x"), PrimIntTypeRepr()),
                                           GenFieldDecl(Ident("y"), PrimIntTypeRepr()),
                                       })),
              GenFieldDecl(
                  Ident("c"),
                  StructRepr({
                      GenFieldDecl(Ident("d"),
                                   StructRepr({GenFieldDecl(Ident("x"), PrimIntTypeRepr()),
                                               GenFieldDecl(Ident("y"), PrimIntTypeRepr())})),
                  })),
          }),
  });

  Sema sema(ctx());
  bool has_errors = sema.Run(root);
  EXPECT_FALSE(has_errors);

  // (1)
  auto s_decl = root->GetDeclarations()[0]->As<ast::StructDeclaration>();
  auto s_decl_repr = s_decl->GetTypeRepr()->SafeAs<ast::StructTypeRepr>();
  auto s_decl_type = s_decl->GetTypeRepr()->GetType()->SafeAs<ast::StructType>();
  EXPECT_NE(nullptr, s_decl_repr);
  EXPECT_NE(nullptr, s_decl_type);
  EXPECT_TRUE(s_decl_type->IsNamed());

  // (2)
  auto s_a_type = s_decl_repr->GetFields()[0]->GetTypeRepr()->GetType()->SafeAs<ast::StructType>();
  auto s_b_type = s_decl_repr->GetFields()[1]->GetTypeRepr()->GetType()->SafeAs<ast::StructType>();
  auto s_c_type = s_decl_repr->GetFields()[2]->GetTypeRepr()->GetType()->SafeAs<ast::StructType>();
  EXPECT_NE(nullptr, s_a_type);
  EXPECT_NE(nullptr, s_b_type);
  EXPECT_NE(nullptr, s_c_type);
  EXPECT_TRUE(s_a_type->IsAnonymous());
  EXPECT_TRUE(s_b_type->IsAnonymous());
  EXPECT_TRUE(s_c_type->IsAnonymous());

  // (3)
  auto s_c_d_type_repr = s_decl_repr->GetFields()[2]
                             ->GetTypeRepr()
                             ->As<ast::StructTypeRepr>()
                             ->GetFields()[0]
                             ->GetTypeRepr();
  auto s_c_d_type = s_c_d_type_repr->GetType()->SafeAs<ast::StructType>();
  EXPECT_EQ(s_b_type, s_c_d_type);
}

}  // namespace tpl::sema
