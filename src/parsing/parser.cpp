#include "parsing/parser.h"

#include <tuple>
#include <unordered_set>
#include <utility>

#include "sema/error_reporter.h"

namespace tpl::parsing {

static std::unordered_set<Token::Type> kTopLevelDecls = {Token::Type::STRUCT,
                                                         Token::Type::FUN};

Parser::Parser(Scanner *scanner, ast::Context *context)
    : scanner_(scanner),
      context_(context),
      node_factory_(context->node_factory()),
      error_reporter_(context->error_reporter()) {}

ast::AstNode *Parser::Parse() {
  util::RegionVector<ast::Decl *> decls(region());

  const SourcePosition &start_pos = scanner()->current_position();

  parsing::ParsingContext pctx;
  while (peek() != Token::Type::EOS) {
    if (ast::Decl *decl = ParseDecl(&pctx)) {
      decls.push_back(decl);
    }
  }

  return node_factory()->NewFile(start_pos, std::move(decls));
}

void Parser::Sync(const std::unordered_set<Token::Type> &s) {
  Next();
  while (peek() != Token::Type::EOS) {
    if (s.count(peek()) > 0) {
      return;
    }
    Next();
  }
}

ast::Decl *Parser::ParseDecl(parsing::ParsingContext *pctx) {
  // At the top-level, we only allow structs and functions
  switch (peek()) {
    case Token::Type::STRUCT: {
      return ParseStructDecl(pctx);
    }
    case Token::Type::FUN: {
      return ParseFunctionDecl(pctx);
    }
    default: {
      break;
    }
  }

  // Report error, sync up and try again
  error_reporter()->Report(scanner()->current_position(),
                           sema::ErrorMessages::kInvalidDeclaration);
  Sync(kTopLevelDecls);
  return nullptr;
}

ast::Decl *Parser::ParseFunctionDecl(parsing::ParsingContext *pctx) {
  Expect(Token::Type::FUN);

  const SourcePosition &position = scanner()->current_position();

  // The function name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The function literal
  auto *fun = ParseFunctionLitExpr(pctx)->As<ast::FunctionLitExpr>();

  // Create declaration
  ast::FunctionDecl *decl =
      node_factory()->NewFunctionDecl(position, name, fun);

  // Done
  return decl;
}

ast::Decl *Parser::ParseStructDecl(parsing::ParsingContext *pctx) {
  Expect(Token::Type::STRUCT);

  const SourcePosition &position = scanner()->current_position();

  // The struct name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type
  auto *struct_type = ParseStructType(pctx)->As<ast::StructTypeRepr>();

  // The declaration object
  ast::StructDecl *decl =
      node_factory()->NewStructDecl(position, name, struct_type);

  // Done
  return decl;
}

ast::Decl *Parser::ParseVariableDecl(parsing::ParsingContext *pctx) {
  // VariableDecl = 'var' Ident ':' Type [ '=' Expr ] ;

  Expect(Token::Type::VAR);

  const SourcePosition &position = scanner()->current_position();

  // The name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = pctx->MakeUniqueSymbol(context(), GetSymbol());

  // The type (if exists)
  ast::Expr *type = nullptr;

  if (Matches(Token::Type::COLON)) {
    type = ParseType(pctx);
  }

  // The initializer (if exists)
  ast::Expr *init = nullptr;

  if (Matches(Token::Type::EQUAL)) {
    init = ParseExpr(pctx);
  }

  if (type == nullptr && init == nullptr) {
    error_reporter()->Report(scanner()->current_position(),
                             sema::ErrorMessages::kMissingTypeAndInitialValue,
                             name);
  }

  // Create declaration object
  ast::VariableDecl *decl =
      node_factory()->NewVariableDecl(position, name, type, init);

  // Done
  return decl;
}

ast::Stmt *Parser::ParseStmt(parsing::ParsingContext *pctx) {
  // Statement = Block | ExpressionStmt | ForStmt | IfStmt | ReturnStmt |
  //             SimpleStmt | VariableDecl ;

  switch (peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStmt(pctx);
    }
    case Token::Type::FOR: {
      return ParseForStmt(pctx);
    }
    case Token::Type::IF: {
      return ParseIfStmt(pctx);
    }
    case Token::Type::RETURN: {
      return ParseReturnStmt(pctx);
    }
    case Token::Type::VAR: {
      ast::Decl *var_decl = ParseVariableDecl(pctx);
      return node_factory()->NewDeclStmt(var_decl);
    }
    default: {
      return ParseSimpleStmt(pctx);
    }
  }
}

ast::Stmt *Parser::ParseSimpleStmt(parsing::ParsingContext *pctx) {
  // SimpleStmt = AssignmentStmt | ExpressionStmt ;
  ast::Expr *left = ParseExpr(pctx);

  if (Matches(Token::Type::EQUAL)) {
    const SourcePosition &pos = scanner()->current_position();
    ast::Expr *right = ParseExpr(pctx);
    return node_factory()->NewAssignmentStmt(pos, left, right);
  }

  return node_factory()->NewExpressionStmt(left);
}

ast::Stmt *Parser::ParseBlockStmt(parsing::ParsingContext *pctx) {
  // BlockStmt = '{' { Stmt } '}' ;

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);
  const SourcePosition &start_position = scanner()->current_position();

  // Where we store all the statements in the block
  util::RegionVector<ast::Stmt *> statements(region());
  statements.reserve(16);

  parsing::ParsingContext nested_pctx = pctx->NewNestedContext();
  // Loop while we don't see the right brace
  while (peek() != Token::Type::RIGHT_BRACE && peek() != Token::Type::EOS) {
    ast::Stmt *stmt = ParseStmt(&nested_pctx);
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);
  const SourcePosition &end_position = scanner()->current_position();

  return node_factory()->NewBlockStmt(start_position, end_position,
                                      std::move(statements));
}

class Parser::ForHeader {
 public:
  // Header for infinite loops
  static ForHeader Infinite() { return ForHeader(); }

  // Header for standard for-loops
  static ForHeader Standard(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next) {
    return ForHeader(init, cond, next, nullptr, nullptr, nullptr);
  }

  // Header for for-in loops
  static ForHeader ForIn(ast::Expr *target, ast::Expr *iter,
                         ast::Attributes *attributes) {
    return ForHeader(nullptr, nullptr, nullptr, target, iter, attributes);
  }

  bool IsForIn() const { return target != nullptr && iter != nullptr; }

  bool IsStandard() const { return !IsForIn(); }

  std::tuple<ast::Stmt *, ast::Expr *, ast::Stmt *> GetForElements() const {
    TPL_ASSERT(IsStandard(), "Loop isn't a standard for-loop");
    return {init, cond, next};
  }

  std::tuple<ast::Expr *, ast::Expr *, ast::Attributes *> GetForInElements()
      const {
    TPL_ASSERT(IsForIn(), "Loop isn't a for-in");
    return {target, iter, attributes};
  }

 private:
  ForHeader(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next,
            ast::Expr *target, ast::Expr *iter, ast::Attributes *attributes)
      : init(init),
        cond(cond),
        next(next),
        target(target),
        iter(iter),
        attributes(attributes) {}

  ForHeader()
      : ForHeader(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr) {}

 private:
  ast::Stmt *init;
  ast::Expr *cond;
  ast::Stmt *next;

  ast::Expr *target;
  ast::Expr *iter;

  ast::Attributes *attributes;
};

Parser::ForHeader Parser::ParseForHeader(parsing::ParsingContext *pctx) {
  // ForHeader = [ '(' ForWhile ')' | '(' ForReg ')' | '(' ForIn ')' ] ;
  //
  // ForWhile = Expr ;
  //
  // ForReg = [ Stmt ] ';' [ Condition ] ';' [ Stmt ] ;
  //
  // ForIn = Expr 'in' Expr [ '[' Attributes ']' ] ;
  // Attributes = Ident '=' Expr ;

  // Infinite loop?
  if (peek() == Token::Type::LEFT_BRACE) {
    return ForHeader::Infinite();
  }

  Expect(Token::Type::LEFT_PAREN);

  ast::Stmt *init = nullptr;
  ast::Expr *cond = nullptr;
  ast::Stmt *next = nullptr;

  init = ParseStmt(pctx);

  // For-in loop?
  if (Matches(Token::Type::IN)) {
    ast::Expr *target = init->SafeAs<ast::ExpressionStmt>()->expression();
    ast::Expr *iter =
        ParseStmt(pctx)->SafeAs<ast::ExpressionStmt>()->expression();

    ast::Attributes *attributes = nullptr;
    if (Matches(Token::Type::AT)) {
      attributes = ParseAttributes(pctx);
    }

    Expect(Token::Type::RIGHT_PAREN);
    return ForHeader::ForIn(target, iter, attributes);
  }

  // Regular for-loop ?
  if (Matches(Token::Type::SEMI)) {
    if (!Matches(Token::Type::SEMI)) {
      cond = ParseExpr(pctx);
      Expect(Token::Type::SEMI);
    }
    if (!Matches(Token::Type::RIGHT_PAREN)) {
      next = ParseStmt(pctx);
      Expect(Token::Type::RIGHT_PAREN);
    }
    return ForHeader::Standard(init, cond, next);
  }

  // While-loop ...
  Expect(Token::Type::RIGHT_PAREN);
  cond = init->SafeAs<ast::ExpressionStmt>()->expression();
  init = next = nullptr;
  return ForHeader::Standard(init, cond, next);
}

ast::Stmt *Parser::ParseForStmt(parsing::ParsingContext *pctx) {
  // ForStmt = 'for' ForHeader Block ;
  Expect(Token::Type::FOR);

  const SourcePosition &position = scanner()->current_position();

  // Parse the header to get the initialization statement, loop condition and
  // next-value statement
  const ForHeader header = ParseForHeader(pctx);

  // Now the loop body
  auto *body = ParseBlockStmt(pctx)->As<ast::BlockStmt>();

  if (header.IsStandard()) {
    const auto &[init, cond, next] = header.GetForElements();
    return node_factory()->NewForStmt(position, init, cond, next, body);
  }
  const auto &[target, iter, attributes] = header.GetForInElements();
  return node_factory()->NewForInStmt(position, target, iter, attributes, body);
}

ast::Stmt *Parser::ParseIfStmt(parsing::ParsingContext *pctx) {
  // IfStmt = 'if' '(' Expr ')' Block [ 'else' ( IfStmt | Block ) ] ;

  Expect(Token::Type::IF);

  const SourcePosition &position = scanner()->current_position();

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  ast::Expr *cond = ParseExpr(pctx);
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  auto *then_stmt = ParseBlockStmt(pctx)->As<ast::BlockStmt>();

  // Handle 'else' statement, if one exists
  ast::Stmt *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    if (peek() == Token::Type::IF) {
      else_stmt = ParseIfStmt(pctx);
    } else {
      else_stmt = ParseBlockStmt(pctx);
    }
  }

  return node_factory()->NewIfStmt(position, cond, then_stmt, else_stmt);
}

ast::Stmt *Parser::ParseReturnStmt(parsing::ParsingContext *pctx) {
  Expect(Token::Type::RETURN);

  const SourcePosition &position = scanner()->current_position();

  ast::Expr *ret = nullptr;
  if (peek() != Token::Type::RIGHT_BRACE) {
    ret = ParseExpr(pctx);
  }

  return node_factory()->NewReturnStmt(position, ret);
}

ast::Expr *Parser::ParseExpr(parsing::ParsingContext *pctx) {
  return ParseBinaryOpExpr(pctx, Token::LowestPrecedence() + 1);
}

ast::Expr *Parser::ParseBinaryOpExpr(parsing::ParsingContext *pctx,
                                     uint32_t min_prec) {
  TPL_ASSERT(min_prec > 0, "The minimum precedence cannot be 0");

  ast::Expr *left = ParseUnaryOpExpr(pctx);

  for (uint32_t prec = Token::GetPrecedence(peek()); prec > min_prec; prec--) {
    // It's possible that we reach a token that has lower precedence than the
    // minimum (e.g., EOS) so we check and early exit
    if (Token::GetPrecedence(peek()) < min_prec) {
      break;
    }

    // Make sure to consume **all** tokens with the same precedence as the
    // current value before moving on to a lower precedence expression. This is
    // to handle cases like 1+2+3+4.
    while (Token::GetPrecedence(peek()) == prec) {
      Token::Type op = Next();
      const SourcePosition &position = scanner()->current_position();
      ast::Expr *right = ParseBinaryOpExpr(pctx, prec);

      if (Token::IsCompareOp(op)) {
        left = node_factory()->NewComparisonOpExpr(position, op, left, right);
      } else {
        left = node_factory()->NewBinaryOpExpr(position, op, left, right);
      }
    }
  }

  return left;
}

ast::Expr *Parser::ParseUnaryOpExpr(parsing::ParsingContext *pctx) {
  // UnaryOpExpr = [ '&' | '!' | '~' | '^' | '-' | '*' ] UnaryOpExpr ;

  switch (peek()) {
    case Token::Type::AMPERSAND:
    case Token::Type::BANG:
    case Token::Type::BIT_NOT:
    case Token::Type::BIT_XOR:
    case Token::Type::MINUS:
    case Token::Type::STAR: {
      Token::Type op = Next();
      const SourcePosition &position = scanner()->current_position();
      ast::Expr *expr = ParseUnaryOpExpr(pctx);
      return node_factory()->NewUnaryOpExpr(position, op, expr);
    }
    default:
      break;
  }

  return ParseLeftHandSideExpression(pctx);
}

ast::Expr *Parser::ParseLeftHandSideExpression(parsing::ParsingContext *pctx) {
  // LeftHandSideExpression = [ CallExpr | MemberExpr | IndexExpr ] ;
  //
  // CallExpr = PrimaryExpr '(' (Expr)* ') ;
  // MemberExpr = PrimaryExpr '.' Expr
  // IndexExpr = PrimaryExpr '[' Expr ']'

  ast::Expr *result = ParsePrimaryExpr(pctx);

  do {
    switch (peek()) {
      case Token::Type::LEFT_PAREN: {
        /*
         * Call expression
         */
        Consume(Token::Type::LEFT_PAREN);
        util::RegionVector<ast::Expr *> args(region());
        while (peek() != Token::Type::RIGHT_PAREN) {
          ast::Expr *arg = ParseExpr(pctx);
          args.push_back(arg);
          if (peek() == Token::Type::COMMA) {
            Next();
          }
        }
        Expect(Token::Type::RIGHT_PAREN);
        result = node_factory()->NewCallExpr(result, std::move(args));
        break;
      }
      case Token::Type::DOT: {
        /*
         * Member expression
         */
        Consume(Token::Type::DOT);
        ast::Expr *member = ParsePrimaryExpr(pctx);
        result =
            node_factory()->NewMemberExpr(result->position(), result, member);
        break;
      }
      case Token::Type::LEFT_BRACKET: {
        /*
         * Index expression (i.e., array or map access)
         */
        Consume(Token::Type::LEFT_BRACKET);
        ast::Expr *index = ParseExpr(pctx);
        Expect(Token::Type::RIGHT_BRACKET);
        result =
            node_factory()->NewIndexExpr(result->position(), result, index);
        break;
      }
      default: {
        /*
         * Not a call, member of index expression. Finish.
         */
        break;
      }
    }
  } while (Token::IsCallOrMemberOrIndex(peek()));

  return result;
}

ast::Expr *Parser::ParsePrimaryExpr(parsing::ParsingContext *pctx) {
  // PrimaryExpr =
  //   nil | 'true' | 'false' | Ident | Number | String | FunctionLiteral |
  //   '(' Expr ')'

  switch (peek()) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory()->NewNilLiteral(scanner()->current_position());
    }
    case Token::Type::TRUE: {
      Consume(Token::Type::TRUE);
      return node_factory()->NewBoolLiteral(scanner()->current_position(),
                                            true);
    }
    case Token::Type::FALSE: {
      Consume(Token::Type::FALSE);
      return node_factory()->NewBoolLiteral(scanner()->current_position(),
                                            false);
    }
    case Token::Type::BUILTIN_IDENTIFIER:
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner()->current_position();
      return node_factory()->NewIdentifierExpr(
          position, pctx->GetScopedSymbol(GetSymbol()));
    }
    case Token::Type::INTEGER: {
      Next();

      // Convert the number
      char *end = nullptr;
      i32 num = std::strtol(GetSymbol().data(), &end, 10);

      const SourcePosition &position = scanner()->current_position();
      return node_factory()->NewIntLiteral(position, num);
    }
    case Token::Type::FLOAT: {
      Next();

      // Convert the number
      char *end = nullptr;
      f32 num = std::strtof(GetSymbol().data(), &end);

      const SourcePosition &position = scanner()->current_position();
      return node_factory()->NewFloatLiteral(position, num);
    }
    case Token::Type::STRING: {
      Next();
      const SourcePosition &position = scanner()->current_position();
      return node_factory()->NewStringLiteral(position, GetSymbol());
    }
    case Token::Type::FUN: {
      Next();
      return ParseFunctionLitExpr(pctx);
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      ast::Expr *expr = ParseExpr(pctx);
      Expect(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: {
      break;
    }
  }

  // Error
  error_reporter()->Report(scanner()->current_position(),
                           sema::ErrorMessages::kExpectingExpression);
  Next();
  return node_factory()->NewBadExpr(scanner()->current_position());
}

ast::Expr *Parser::ParseFunctionLitExpr(parsing::ParsingContext *pctx) {
  // FunctionLiteralExpr = Signature FunctionBody ;
  //
  // FunctionBody = Block ;

  // Parse the type
  auto *func_type = ParseFunctionType(pctx)->As<ast::FunctionTypeRepr>();

  // Parse the body
  auto *body = ParseBlockStmt(pctx)->As<ast::BlockStmt>();

  // Done
  return node_factory()->NewFunctionLitExpr(func_type, body);
}

ast::Expr *Parser::ParseType(parsing::ParsingContext *pctx) {
  switch (peek()) {
    case Token::Type::NIL:
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner()->current_position();
      return node_factory()->NewIdentifierExpr(position, GetSymbol());
    }
    case Token::Type::MAP: {
      return ParseMapType(pctx);
    }
    case Token::Type::LEFT_PAREN: {
      return ParseFunctionType(pctx);
    }
    case Token::Type::STAR: {
      return ParsePointerType(pctx);
    }
    case Token::Type::LEFT_BRACKET: {
      return ParseArrayType(pctx);
    }
    case Token::Type::STRUCT: {
      return ParseStructType(pctx);
    }
    default: {
      break;
    }
  }

  // Error
  error_reporter()->Report(scanner()->current_position(),
                           sema::ErrorMessages::kExpectingType);

  return nullptr;
}

ast::Expr *Parser::ParseFunctionType(parsing::ParsingContext *pctx) {
  // FuncType = '(' { ParameterList } ')' '->' Type ;
  // ParameterList = { Ident ':' } Type ;

  Consume(Token::Type::LEFT_PAREN);

  const SourcePosition &position = scanner()->current_position();

  util::RegionVector<ast::FieldDecl *> params(region());
  params.reserve(4);

  while (peek() != Token::Type::RIGHT_PAREN) {
    const SourcePosition &field_position = scanner()->current_position();

    ast::Identifier ident(nullptr);

    ast::Expr *type = nullptr;

    if (Matches(Token::Type::IDENTIFIER)) {
      ident = GetSymbol();
    }

    if (Matches(Token::Type::COLON) || ident.data() == nullptr) {
      type = ParseType(pctx);
    } else {
      type = node_factory()->NewIdentifierExpr(field_position, ident);
      ident = ast::Identifier(nullptr);
    }

    // That's it
    params.push_back(node_factory()->NewFieldDecl(field_position, ident, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  ast::Expr *ret = ParseType(pctx);

  return node_factory()->NewFunctionType(position, std::move(params), ret);
}

ast::Expr *Parser::ParsePointerType(parsing::ParsingContext *pctx) {
  // PointerTypeRepr = '*' Type ;

  Expect(Token::Type::STAR);

  const SourcePosition &position = scanner()->current_position();

  ast::Expr *base = ParseType(pctx);

  return node_factory()->NewPointerType(position, base);
}

ast::Expr *Parser::ParseArrayType(parsing::ParsingContext *pctx) {
  // ArrayTypeRepr = '[' [ Length ] ']' Type ;
  // Length = Expr ;

  Consume(Token::Type::LEFT_BRACKET);

  const SourcePosition &position = scanner()->current_position();

  // If the next token doesn't match a right bracket, it means we have a length
  ast::Expr *len = nullptr;
  if (!Matches(Token::Type::RIGHT_BRACKET)) {
    len = ParseExpr(pctx);
    Expect(Token::Type::RIGHT_BRACKET);
  }

  // Now the type
  ast::Expr *elem_type = ParseType(pctx);

  // Done
  return node_factory()->NewArrayType(position, len, elem_type);
}

ast::Expr *Parser::ParseStructType(parsing::ParsingContext *pctx) {
  // StructType = '{' { Ident ':' Type } '}' ;

  const SourcePosition &position = scanner()->current_position();

  Consume(Token::Type::LEFT_BRACE);

  util::RegionVector<ast::FieldDecl *> fields(region());

  while (peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);

    const SourcePosition &field_position = scanner()->current_position();

    // The parameter name
    ast::Identifier name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expr *type = ParseType(pctx);

    // That's it
    fields.push_back(node_factory()->NewFieldDecl(field_position, name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory()->NewStructType(position, std::move(fields));
}

ast::Expr *Parser::ParseMapType(parsing::ParsingContext *pctx) {
  // MapType = 'map' '[' Expr ']' Expr ;

  const SourcePosition &position = scanner()->current_position();

  Consume(Token::Type::MAP);

  Expect(Token::Type::LEFT_BRACKET);

  ast::Expr *key_type = ParseType(pctx);

  Expect(Token::Type::RIGHT_BRACKET);

  ast::Expr *value_type = ParseType(pctx);

  return node_factory()->NewMapType(position, key_type, value_type);
}

ast::Attributes *Parser::ParseAttributes(parsing::ParsingContext *pctx) {
  util::RegionUnorderedMap<ast::Identifier, ast::Expr *> attrs(region());

  Consume(Token::Type::LEFT_BRACKET);

  while (peek() != Token::Type::RIGHT_BRACKET) {
    // First, the attribute name
    Expect(Token::Type::IDENTIFIER);
    ast::Identifier attribute_name = GetSymbol();

    // Then '='
    Expect(Token::Type::EQUAL);

    // Then value
    ast::Expr *expr = ParsePrimaryExpr(pctx);

    attrs.emplace(attribute_name, expr);
  }

  Consume(Token::Type::RIGHT_BRACKET);

  return new (region()) ast::Attributes(std::move(attrs));
}

}  // namespace tpl::parsing
