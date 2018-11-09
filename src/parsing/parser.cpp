#include "parsing/parser.h"

#include "sema/error_reporter.h"

namespace tpl::parsing {

Parser::Parser(Scanner &scanner, ast::AstContext &ast_context)
    : scanner_(scanner),
      ast_context_(ast_context),
      node_factory_(ast_context.node_factory()),
      error_reporter_(ast_context.error_reporter()) {}

ast::AstNode *Parser::Parse() {
  util::RegionVector<ast::Decl *> decls(region());

  const SourcePosition &start_pos = scanner().current_position();

  while (peek() != Token::Type::EOS) {
    decls.push_back(ParseDecl());
  }

  return node_factory().NewFile(start_pos, std::move(decls));
}

ast::Decl *Parser::ParseDecl() {
  // At the top-level, we only allow structs and functions
  switch (peek()) {
    case Token::Type::STRUCT: {
      return ParseStructDecl();
    }
    case Token::Type::FUN: {
      return ParseFunctionDecl();
    }
    default: { return nullptr; }
  }
}

ast::Decl *Parser::ParseFunctionDecl() {
  Expect(Token::Type::FUN);

  const SourcePosition &position = scanner().current_position();

  // The function name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The function literal
  auto *fun = ParseFunctionLitExpr()->As<ast::FunctionLitExpr>();

  // Create declaration
  ast::FunctionDecl *decl = node_factory().NewFunctionDecl(position, name, fun);

  // Done
  return decl;
}

ast::Decl *Parser::ParseStructDecl() {
  Expect(Token::Type::STRUCT);

  const SourcePosition &position = scanner().current_position();

  // The struct name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type
  auto *struct_type = ParseStructType()->As<ast::StructTypeRepr>();

  // The declaration object
  ast::StructDecl *decl =
      node_factory().NewStructDecl(position, name, struct_type);

  // Done
  return decl;
}

ast::Decl *Parser::ParseVariableDecl() {
  // VariableDecl = 'var' Ident ':' Type [ '=' Expr ] ;

  Expect(Token::Type::VAR);

  const SourcePosition &position = scanner().current_position();

  // The name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type (if exists)
  ast::Expr *type = nullptr;

  if (Matches(Token::Type::COLON)) {
    type = ParseType();
  }

  // The initializer (if exists)
  ast::Expr *init = nullptr;

  if (Matches(Token::Type::EQUAL)) {
    init = ParseExpr();
  }

  if (type == nullptr && init == nullptr) {
    error_reporter().Report(scanner().current_position(),
                            sema::ErrorMessages::kMissingTypeAndInitialValue,
                            name);
  }

  // Create declaration object
  ast::VariableDecl *decl =
      node_factory().NewVariableDecl(position, name, type, init);

  // Done
  return decl;
}

ast::Stmt *Parser::ParseStmt() {
  // Statement = Block | ExpressionStmt | ForStmt | IfStmt | ReturnStmt |
  //             SimpleStmt | VariableDecl ;

  switch (peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStmt();
    }
    case Token::Type::FOR: {
      return ParseForStmt();
    }
    case Token::Type::IF: {
      return ParseIfStmt();
    }
    case Token::Type::RETURN: {
      return ParseReturnStmt();
    }
    case Token::Type::VAR: {
      ast::Decl *var_decl = ParseVariableDecl();
      return node_factory().NewDeclStmt(var_decl);
    }
    default: { return ParseSimpleStmt(); }
  }
}

ast::Stmt *Parser::ParseSimpleStmt() {
  // SimpleStmt = AssignmentStmt | ExpressionStmt ;
  ast::Expr *left = ParseExpr();

  if (Matches(Token::Type::EQUAL)) {
    const SourcePosition &pos = scanner().current_position();
    ast::Expr *right = ParseExpr();
    return node_factory().NewAssignmentStmt(pos, left, right);
  }

  return node_factory().NewExpressionStmt(left);
}

ast::Stmt *Parser::ParseBlockStmt() {
  // BlockStmt = '{' { Stmt } '}' ;

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);
  const SourcePosition &start_position = scanner().current_position();

  // Where we store all the statements in the block
  util::RegionVector<ast::Stmt *> statements(region());
  statements.reserve(16);

  // Loop while we don't see the right brace
  while (peek() != Token::Type::RIGHT_BRACE && peek() != Token::Type::EOS) {
    ast::Stmt *stmt = ParseStmt();
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);
  const SourcePosition &end_position = scanner().current_position();

  return node_factory().NewBlockStmt(start_position, end_position,
                                     std::move(statements));
}

class Parser::ForHeader {
 public:
  // Header for infinite loops
  static ForHeader Infinite() { return ForHeader(); }

  // Header for standard for-loops
  static ForHeader Standard(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next) {
    return ForHeader(init, cond, next, nullptr, nullptr);
  }

  // Header for for-in loops
  static ForHeader ForIn(ast::Expr *target, ast::Expr *iter) {
    return ForHeader(nullptr, nullptr, nullptr, target, iter);
  }

  bool IsForIn() const { return target != nullptr && iter != nullptr; }

  bool IsStandard() const { return !IsForIn(); }

  std::tuple<ast::Stmt *, ast::Expr *, ast::Stmt *> GetForElements() const {
    TPL_ASSERT(IsStandard(), "Loop isn't a standard for-loop");
    return {init, cond, next};
  }

  std::tuple<ast::Expr *, ast::Expr *> GetForInElements() const {
    TPL_ASSERT(IsForIn(), "Loop isn't a for-in");
    return {target, iter};
  }

 private:
  ForHeader(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next,
            ast::Expr *target, ast::Expr *iter)
      : init(init), cond(cond), next(next), target(target), iter(iter) {}

  ForHeader() : ForHeader(nullptr, nullptr, nullptr, nullptr, nullptr) {}

 private:
  ast::Stmt *init;
  ast::Expr *cond;
  ast::Stmt *next;

  ast::Expr *target;
  ast::Expr *iter;
};

Parser::ForHeader Parser::ParseForHeader() {
  // ForStmt = 'for' '(' [ Condition | ForHeader | ForInHeader] ')' Block ;
  //
  // Condition = Expr ;
  //
  // ForHeader = [ Stmt ] ';' [ Condition ] ';' [ Stmt ] ;
  //
  // ForInHeader = [ Expr ] 'in' [ Expr ] ;

  Expect(Token::Type::LEFT_PAREN);

  // Infinite loop ?
  if (Matches(Token::Type::RIGHT_PAREN)) {
    return ForHeader::Infinite();
  }

  ast::Stmt *init = nullptr;
  ast::Expr *cond = nullptr;
  ast::Stmt *next = nullptr;

  init = ParseStmt();

  // For-in loop?
  if (Matches(Token::Type::IN)) {
    ast::Expr *target = init->SafeAs<ast::ExpressionStmt>()->expression();
    ast::Expr *iter = ParseStmt()->SafeAs<ast::ExpressionStmt>()->expression();
    Expect(Token::Type::RIGHT_PAREN);
    return ForHeader::ForIn(target, iter);
  }

  // Regular for-loop ?
  if (Matches(Token::Type::SEMI)) {
    if (!Matches(Token::Type::SEMI)) {
      cond = ParseExpr();
      Expect(Token::Type::SEMI);
    }
    if (!Matches(Token::Type::RIGHT_PAREN)) {
      next = ParseStmt();
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

ast::Stmt *Parser::ParseForStmt() {
  Expect(Token::Type::FOR);

  const SourcePosition &position = scanner().current_position();

  // Parse the header to get the initialization statement, loop condition and
  // next-value statement
  const ForHeader &header = ParseForHeader();

  // Now the loop body
  auto *body = ParseBlockStmt()->As<ast::BlockStmt>();

  if (header.IsStandard()) {
    const auto &[init, cond, next] = header.GetForElements();
    return node_factory().NewForStmt(position, init, cond, next, body);
  } else {
    const auto &[target, iter] = header.GetForInElements();
    return node_factory().NewForInStmt(position, target, iter, body);
  }
}

ast::Stmt *Parser::ParseIfStmt() {
  // IfStmt = 'if' '(' Expr ')' Block [ 'else' ( IfStmt | Block ) ] ;

  Expect(Token::Type::IF);

  const SourcePosition &position = scanner().current_position();

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  ast::Expr *cond = ParseExpr();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  auto *then_stmt = ParseBlockStmt()->As<ast::BlockStmt>();

  // Handle 'else' statement, if one exists
  ast::Stmt *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    if (peek() == Token::Type::IF) {
      else_stmt = ParseIfStmt();
    } else {
      else_stmt = ParseBlockStmt();
    }
  }

  return node_factory().NewIfStmt(position, cond, then_stmt, else_stmt);
}

ast::Stmt *Parser::ParseReturnStmt() {
  Expect(Token::Type::RETURN);

  const SourcePosition &position = scanner().current_position();

  ast::Expr *ret = ParseExpr();

  return node_factory().NewReturnStmt(position, ret);
}

ast::Expr *Parser::ParseExpr() {
  return ParseBinaryOpExpr(Token::LowestPrecedence() + 1);
}

ast::Expr *Parser::ParseBinaryOpExpr(uint32_t min_prec) {
  TPL_ASSERT(min_prec > 0, "The minimum precedence cannot be 0");

  ast::Expr *left = ParseUnaryOpExpr();

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
      const SourcePosition &position = scanner().current_position();
      ast::Expr *right = ParseBinaryOpExpr(prec);

      if (Token::IsCompareOp(op)) {
        left = node_factory().NewComparisonOpExpr(position, op, left, right);
      } else {
        left = node_factory().NewBinaryOpExpr(position, op, left, right);
      }
    }
  }

  return left;
}

ast::Expr *Parser::ParseUnaryOpExpr() {
  // UnaryOpExpr = [ '&' | '!' | '~' | '-' | '*' ] UnaryOpExpr ;

  switch (peek()) {
    case Token::Type::AMPERSAND:
    case Token::Type::BANG:
    case Token::Type::BIT_NOT:
    case Token::Type::MINUS:
    case Token::Type::STAR: {
      Token::Type op = Next();
      const SourcePosition &position = scanner().current_position();
      ast::Expr *expr = ParseUnaryOpExpr();
      return node_factory().NewUnaryOpExpr(position, op, expr);
    }
    default:
      break;
  }

  return ParseLeftHandSideExpression();
}

ast::Expr *Parser::ParseLeftHandSideExpression() {
  // LeftHandSideExpression = [ CallExpr | MemberExpr | IndexExpr ] ;
  //
  // CallExpr = PrimaryExpr '(' (Expr)* ') ;
  // MemberExpr = PrimaryExpr '.' Expr
  // IndexExpr = PrimaryExpr '[' Expr ']'

  ast::Expr *result = ParsePrimaryExpr();

  switch (peek()) {
    case Token::Type::LEFT_PAREN: {
      /*
       * Call expression
       */
      Consume(Token::Type::LEFT_PAREN);

      util::RegionVector<ast::Expr *> args(region());

      bool done = (peek() == Token::Type::RIGHT_PAREN);
      while (!done) {
        // Parse argument
        ast::Expr *arg = ParseExpr();
        args.push_back(arg);

        done = (peek() != Token::Type::COMMA);
        if (!done) {
          Next();
        }
      }

      Expect(Token::Type::RIGHT_PAREN);

      result = node_factory().NewCallExpr(result, std::move(args));
      break;
    }
    case Token::Type::DOT: {
      /*
       * Member expression
       */
      Consume(Token::Type::DOT);

      // Eagerly consume the full selector chain
      ast::Expr *expr = result;
      do {
        ast::Expr *member = ParsePrimaryExpr();
        expr = node_factory().NewMemberExpr(result->position(), expr, member);
      } while (Matches(Token::Type::DOT));

      result = expr;
      break;
    }
    case Token::Type::LEFT_BRACKET: {
      /*
       * Index expression (i.e., array or map access)
       */
      Consume(Token::Type::LEFT_BRACKET);
      ast::Expr *index = ParseExpr();
      Expect(Token::Type::RIGHT_BRACKET);
      result = node_factory().NewIndexExpr(result->position(), result, index);
      break;
    }
    default: { break; }
  }

  return result;
}

ast::Expr *Parser::ParsePrimaryExpr() {
  // PrimaryExpr =
  //   nil | 'true' | 'false' | Ident | Number | String | FunctionLiteral |
  //   '(' Expr ')'

  switch (peek()) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory().NewNilLiteral(scanner().current_position());
    }
    case Token::Type::TRUE: {
      Consume(Token::Type::TRUE);
      return node_factory().NewBoolLiteral(scanner().current_position(), true);
    }
    case Token::Type::FALSE: {
      Consume(Token::Type::FALSE);
      return node_factory().NewBoolLiteral(scanner().current_position(), false);
    }
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewIdentifierExpr(position, GetSymbol());
    }
    case Token::Type::INTEGER: {
      Next();

      // Convert the number
      char *end = nullptr;
      i32 num = std::strtol(GetSymbol().data(), &end, 10);

      const SourcePosition &position = scanner().current_position();
      return node_factory().NewIntLiteral(position, num);
    }
    case Token::Type::FLOAT: {
      Next();

      // Convert the number
      char *end = nullptr;
      f32 num = std::strtof(GetSymbol().data(), &end);

      const SourcePosition &position = scanner().current_position();
      return node_factory().NewFloatLiteral(position, num);
    }
    case Token::Type::STRING: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewStringLiteral(position, GetSymbol());
    }
    case Token::Type::FUN: {
      Next();
      return ParseFunctionLitExpr();
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      ast::Expr *expr = ParseExpr();
      Expect(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: { break; }
  }

  // Error
  error_reporter().Report(scanner().current_position(),
                          sema::ErrorMessages::kExpectingExpression);
  Next();
  return node_factory().NewBadExpr(scanner().current_position());
}

ast::Expr *Parser::ParseFunctionLitExpr() {
  // FunctionLiteralExpr = Signature FunctionBody ;
  //
  // FunctionBody = Block ;

  // Parse the type
  auto *func_type = ParseFunctionType()->As<ast::FunctionTypeRepr>();

  // Parse the body
  auto *body = ParseBlockStmt()->As<ast::BlockStmt>();

  // Done
  return node_factory().NewFunctionLitExpr(func_type, body);
}

ast::Expr *Parser::ParseType() {
  switch (peek()) {
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewIdentifierExpr(position, GetSymbol());
    }
    case Token::Type::LEFT_PAREN: {
      return ParseFunctionType();
    }
    case Token::Type::STAR: {
      return ParsePointerType();
    }
    case Token::Type::LEFT_BRACKET: {
      return ParseArrayType();
    }
    case Token::Type::STRUCT: {
      return ParseStructType();
    }
    default: { break; }
  }

  // Error
  error_reporter().Report(scanner().current_position(),
                          sema::ErrorMessages::kExpectingType);

  return nullptr;
}

ast::Expr *Parser::ParseFunctionType() {
  // FuncType = '(' { Ident ':' Type } ')' '->' Type ;

  Consume(Token::Type::LEFT_PAREN);

  const SourcePosition &position = scanner().current_position();

  util::RegionVector<ast::FieldDecl *> params(region());

  while (true) {
    if (!Matches(Token::Type::IDENTIFIER)) {
      break;
    }

    const SourcePosition &field_position = scanner().current_position();

    // The parameter name
    ast::Identifier name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expr *type = ParseType();

    // That's it
    params.push_back(node_factory().NewFieldDecl(field_position, name, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  ast::Expr *ret = ParseType();

  return node_factory().NewFunctionType(position, std::move(params), ret);
}

ast::Expr *Parser::ParsePointerType() {
  // PointerTypeRepr = '*' Type ;

  Expect(Token::Type::STAR);

  const SourcePosition &position = scanner().current_position();

  ast::Expr *base = ParseType();

  return node_factory().NewPointerType(position, base);
}

ast::Expr *Parser::ParseArrayType() {
  // ArrayTypeRepr = '[' [ Length ] ']' Type ;
  // Length = Expr ;

  Consume(Token::Type::LEFT_BRACKET);

  const SourcePosition &position = scanner().current_position();

  // If the next token doesn't match a right bracket, it means we have a length
  ast::Expr *len = nullptr;
  if (!Matches(Token::Type::RIGHT_BRACKET)) {
    len = ParseExpr();
    Expect(Token::Type::RIGHT_BRACKET);
  }

  // Now the type
  ast::Expr *elem_type = ParseType();

  // Done
  return node_factory().NewArrayType(position, len, elem_type);
}

ast::Expr *Parser::ParseStructType() {
  // StructType = '{' { Ident ':' Type } '}' ;

  Consume(Token::Type::LEFT_BRACE);

  const SourcePosition &position = scanner().current_position();

  util::RegionVector<ast::FieldDecl *> fields(region());

  while (peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);

    const SourcePosition &field_position = scanner().current_position();

    // The parameter name
    ast::Identifier name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expr *type = ParseType();

    // That's it
    fields.push_back(node_factory().NewFieldDecl(field_position, name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory().NewStructType(position, std::move(fields));
}

}  // namespace tpl::parsing
