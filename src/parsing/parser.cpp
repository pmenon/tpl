#include "parsing/parser.h"

#include <tuple>
#include <unordered_set>
#include <utility>

#include "sema/error_reporter.h"

namespace tpl::parsing {

static std::unordered_set<Token::Type> kTopLevelDecls = {Token::Type::STRUCT, Token::Type::FUN};

Parser::Parser(Scanner *scanner, ast::Context *context)
    : scanner_(scanner),
      context_(context),
      node_factory_(context->GetNodeFactory()),
      error_reporter_(context->GetErrorReporter()) {}

ast::AstNode *Parser::Parse() {
  util::RegionVector<ast::Declaration *> decls(Region());

  const SourcePosition &start_pos = scanner_->CurrentPosition();

  while (Peek() != Token::Type::EOS) {
    if (ast::Declaration *decl = ParseDeclaration()) {
      decls.push_back(decl);
    }
  }

  return node_factory_->NewFile(start_pos, std::move(decls));
}

void Parser::Sync(const std::unordered_set<Token::Type> &s) {
  Next();
  while (Peek() != Token::Type::EOS) {
    if (s.count(Peek()) > 0) {
      return;
    }
    Next();
  }
}

ast::Expression *Parser::MakeExpr(ast::AstNode *node) {
  if (node == nullptr) {
    return nullptr;
  }

  if (auto *expr_stmt = node->SafeAs<ast::ExpressionStatement>()) {
    return expr_stmt->GetExpression();
  }

  const auto err_msg = sema::ErrorMessages::kExpectingExpression;
  error_reporter_->Report(node->Position(), err_msg);
  return nullptr;
}

ast::Declaration *Parser::ParseDeclaration() {
  // At the top-level, we only allow structs and functions
  switch (Peek()) {
    case Token::Type::STRUCT: {
      return ParseStructDeclaration();
    }
    case Token::Type::FUN: {
      return ParseFunctionDeclaration();
    }
    default: {
      break;
    }
  }

  // Report error, sync up and try again
  error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kInvalidDeclaration);
  Sync(kTopLevelDecls);
  return nullptr;
}

ast::Declaration *Parser::ParseFunctionDeclaration() {
  Expect(Token::Type::FUN);

  const SourcePosition &position = scanner_->CurrentPosition();

  // The function name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The function literal
  auto *fun = ParseFunctionLiteralExpression()->As<ast::FunctionLiteralExpression>();

  // Create declaration
  ast::FunctionDeclaration *decl = node_factory_->NewFunctionDeclaration(position, name, fun);

  // Done
  return decl;
}

ast::Declaration *Parser::ParseStructDeclaration() {
  Expect(Token::Type::STRUCT);

  const SourcePosition &position = scanner_->CurrentPosition();

  // The struct name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type
  auto *struct_type = ParseStructType()->As<ast::StructTypeRepr>();

  // The declaration object
  ast::StructDeclaration *decl = node_factory_->NewStructDeclaration(position, name, struct_type);

  // Done
  return decl;
}

ast::Declaration *Parser::ParseVariableDeclaration() {
  // VariableDecl = 'var' Ident ':' Type [ '=' Expression ] ;

  Expect(Token::Type::VAR);

  const SourcePosition &position = scanner_->CurrentPosition();

  // The name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type (if exists)
  ast::Expression *type = nullptr;

  if (Matches(Token::Type::COLON)) {
    type = ParseType();
  }

  // The initializer (if exists)
  ast::Expression *init = nullptr;

  if (Matches(Token::Type::EQUAL)) {
    init = ParseExpression();
  }

  if (type == nullptr && init == nullptr) {
    error_reporter_->Report(scanner_->CurrentPosition(),
                            sema::ErrorMessages::kMissingTypeAndInitialValue, name);
  }

  // Create declaration object
  ast::VariableDeclaration *decl =
      node_factory_->NewVariableDeclaration(position, name, type, init);

  // Done
  return decl;
}

ast::Statement *Parser::ParseStatement() {
  // Statement = Block | ExpressionStatement | ForStatement | IfStatement | ReturnStatement |
  //             SimpleStatement | VariableDecl ;

  switch (Peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStatement();
    }
    case Token::Type::FOR: {
      return ParseForStatement();
    }
    case Token::Type::IF: {
      return ParseIfStatement();
    }
    case Token::Type::RETURN: {
      return ParseReturnStatement();
    }
    case Token::Type::VAR: {
      ast::Declaration *var_decl = ParseVariableDeclaration();
      return node_factory_->NewDeclStatement(var_decl);
    }
    default: {
      return ParseSimpleStatement();
    }
  }
}

ast::Statement *Parser::ParseSimpleStatement() {
  // SimpleStatement = AssignmentStatement | ExpressionStatement ;
  // AssignmentStatement = Expression '=' Expression ;
  // ExpressionStatement = Expression ;
  ast::Expression *left = ParseExpression();

  if (Matches(Token::Type::EQUAL)) {
    const SourcePosition &pos = scanner_->CurrentPosition();
    ast::Expression *right = ParseExpression();
    return node_factory_->NewAssignmentStatement(pos, left, right);
  }

  return node_factory_->NewExpressionStatement(left);
}

ast::Statement *Parser::ParseBlockStatement() {
  // BlockStatement = '{' { Statement } '}' ;

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);
  const SourcePosition &start_position = scanner_->CurrentPosition();

  // Where we store all the statements in the block
  util::RegionVector<ast::Statement *> statements(Region());
  statements.reserve(16);

  // Loop while we don't see the right brace
  while (Peek() != Token::Type::RIGHT_BRACE && Peek() != Token::Type::EOS) {
    ast::Statement *stmt = ParseStatement();
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);
  const SourcePosition &end_position = scanner_->CurrentPosition();

  return node_factory_->NewBlockStatement(start_position, end_position, std::move(statements));
}

class Parser::ForHeader {
 public:
  // Header for infinite loops
  static ForHeader Infinite() { return ForHeader(); }

  // Header for standard for-loops
  static ForHeader Standard(ast::Statement *init, ast::Expression *cond, ast::Statement *next) {
    return ForHeader(init, cond, next, nullptr, nullptr);
  }

  // Header for for-in loops
  static ForHeader ForIn(ast::Expression *target, ast::Expression *iter) {
    return ForHeader(nullptr, nullptr, nullptr, target, iter);
  }

  bool IsForIn() const { return target_ != nullptr && iter_ != nullptr; }

  bool IsStandard() const { return !IsForIn(); }

  std::tuple<ast::Statement *, ast::Expression *, ast::Statement *> GetForElements() const {
    TPL_ASSERT(IsStandard(), "Loop isn't a standard for-loop");
    return {init_, cond_, next_};
  }

  std::tuple<ast::Expression *, ast::Expression *> GetForInElements() const {
    TPL_ASSERT(IsForIn(), "Loop isn't a for-in");
    return {target_, iter_};
  }

 private:
  ForHeader(ast::Statement *init, ast::Expression *cond, ast::Statement *next,
            ast::Expression *target, ast::Expression *iter)
      : init_(init), cond_(cond), next_(next), target_(target), iter_(iter) {}

  ForHeader() : ForHeader(nullptr, nullptr, nullptr, nullptr, nullptr) {}

 private:
  ast::Statement *init_;
  ast::Expression *cond_;
  ast::Statement *next_;

  ast::Expression *target_;
  ast::Expression *iter_;
};

Parser::ForHeader Parser::ParseForHeader() {
  // ForHeader = [ '(' ForWhile ')' | '(' ForReg ')' | '(' ForIn ')' ] .
  // ForWhile = Expression .
  // ForReg = [ Statement ] ';' [ Expression ] ';' [ Statement ] .
  // ForIn = Expression 'in' Expression [ '[' Attributes ']' ] .
  // Attributes = { Ident '=' Expression } .

  // Infinite loop?
  if (Peek() == Token::Type::LEFT_BRACE) {
    return ForHeader::Infinite();
  }

  Expect(Token::Type::LEFT_PAREN);

  ast::Statement *init = nullptr, *cond = nullptr, *next = nullptr;

  if (Peek() != Token::Type::SEMI) {
    cond = ParseStatement();
  }

  // If we see an 'in', it's a for-in loop
  if (Matches(Token::Type::IN)) {
    TPL_ASSERT(cond != nullptr, "Must have parsed can't be null");
    ast::Expression *target = MakeExpr(cond);
    ast::Expression *iter = MakeExpr(ParseStatement());
    Expect(Token::Type::RIGHT_PAREN);
    return ForHeader::ForIn(target, iter);
  }

  // Parse either regular for or for-while
  if (Matches(Token::Type::SEMI)) {
    init = cond;
    cond = nullptr;
    if (Peek() != Token::Type::SEMI) {
      cond = ParseStatement();
    }
    Expect(Token::Type::SEMI);
    if (Peek() != Token::Type::RIGHT_PAREN) {
      next = ParseStatement();
    }
  }

  Expect(Token::Type::RIGHT_PAREN);

  return ForHeader::Standard(init, MakeExpr(cond), next);
}

ast::Statement *Parser::ParseForStatement() {
  // ForStatement = 'for' ForHeader Block ;
  Expect(Token::Type::FOR);

  const SourcePosition &position = scanner_->CurrentPosition();

  // Parse the header to get the initialization statement, loop condition and
  // next-value statement
  const ForHeader header = ParseForHeader();

  // Now the loop body
  auto *body = ParseBlockStatement()->As<ast::BlockStatement>();

  if (header.IsStandard()) {
    const auto &[init, cond, next] = header.GetForElements();
    return node_factory_->NewForStatement(position, init, cond, next, body);
  }

  const auto &[target, iter] = header.GetForInElements();
  return node_factory_->NewForInStatement(position, target, iter, body);
}

ast::Statement *Parser::ParseIfStatement() {
  // IfStatement = 'if' '(' Expression ')' Block [ 'else' ( IfStatement | Block ) ] ;

  Expect(Token::Type::IF);

  const SourcePosition &position = scanner_->CurrentPosition();

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  ast::Expression *cond = ParseExpression();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  auto *then_stmt = ParseBlockStatement()->As<ast::BlockStatement>();

  // Handle 'else' statement, if one exists
  ast::Statement *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    if (Peek() == Token::Type::IF) {
      else_stmt = ParseIfStatement();
    } else {
      else_stmt = ParseBlockStatement();
    }
  }

  return node_factory_->NewIfStatement(position, cond, then_stmt, else_stmt);
}

ast::Statement *Parser::ParseReturnStatement() {
  Expect(Token::Type::RETURN);

  const SourcePosition &position = scanner_->CurrentPosition();

  ast::Expression *ret = nullptr;
  if (Peek() != Token::Type::RIGHT_BRACE) {
    ret = ParseExpression();
  }

  return node_factory_->NewReturnStatement(position, ret);
}

ast::Expression *Parser::ParseExpression() {
  return ParseBinaryOpExpression(Token::LowestPrecedence() + 1);
}

ast::Expression *Parser::ParseBinaryOpExpression(uint32_t min_prec) {
  TPL_ASSERT(min_prec > 0, "The minimum precedence cannot be 0");

  ast::Expression *left = ParseUnaryOpExpression();

  for (uint32_t prec = Token::GetPrecedence(Peek()); prec > min_prec; prec--) {
    // It's possible that we reach a token that has lower precedence than the
    // minimum (e.g., EOS) so we check and early exit
    if (Token::GetPrecedence(Peek()) < min_prec) {
      break;
    }

    // Make sure to consume **all** tokens with the same precedence as the
    // current value before moving on to a lower precedence expression. This is
    // to handle cases like 1+2+3+4.
    while (Token::GetPrecedence(Peek()) == prec) {
      Token::Type op = Next();
      const SourcePosition &position = scanner_->CurrentPosition();
      ast::Expression *right = ParseBinaryOpExpression(prec);

      if (Token::IsCompareOp(op)) {
        left = node_factory_->NewComparisonOpExpression(position, op, left, right);
      } else {
        left = node_factory_->NewBinaryOpExpression(position, op, left, right);
      }
    }
  }

  return left;
}

ast::Expression *Parser::ParseUnaryOpExpression() {
  // UnaryOpExpr = PrimaryExpr | unary_op UnaryOpExpr ;
  // unary_op = '&' | '!' | '~' | '^' | '-' | '+' | '*'

  switch (Peek()) {
    case Token::Type::AMPERSAND:
    case Token::Type::BANG:
    case Token::Type::BIT_NOT:
    case Token::Type::BIT_XOR:
    case Token::Type::MINUS:
    case Token::Type::PLUS:
    case Token::Type::STAR: {
      Token::Type op = Next();
      SourcePosition pos = scanner_->CurrentPosition();
      ast::Expression *expression = ParseUnaryOpExpression();

      // Some quick rewrites here.
      if (const auto literal = expression->SafeAs<ast::LiteralExpression>()) {
        if (op == Token::Type::BANG && literal->IsBoolLiteral()) {
          // Negate the boolean value and return a new literal.
          return node_factory_->NewBoolLiteral(pos, !literal->BoolVal());
        } else if (literal->IsIntegerLiteral()) {
          // Simple rewrites involving numbers.
          switch (op) {
            case Token::Type::PLUS:
              return expression;
            case Token::Type::MINUS:
              return node_factory_->NewIntLiteral(pos, -literal->IntegerVal());
            default:
              break;
          }
        }
      }

      return node_factory_->NewUnaryOpExpression(pos, op, expression);
    }
    default: {
      break;
    }
  }

  return ParsePrimaryExpression();
}

ast::Expression *Parser::ParsePrimaryExpression() {
  // PrimaryExpr = Operand | CallExpr | MemberExpr | IndexExpr ;
  // CallExpr = PrimaryExpr '(' (Expression)* ') ;
  // MemberExpr = PrimaryExpr '.' Expression
  // IndexExpr = PrimaryExpr '[' Expression ']'

  ast::Expression *result = ParseOperand();

  do {
    switch (Peek()) {
      case Token::Type::LEFT_PAREN: {
        // Call expression
        Consume(Token::Type::LEFT_PAREN);
        util::RegionVector<ast::Expression *> args(Region());
        while (Peek() != Token::Type::RIGHT_PAREN) {
          ast::Expression *arg = ParseExpression();
          args.push_back(arg);
          if (Peek() == Token::Type::COMMA) {
            Next();
          }
        }
        Expect(Token::Type::RIGHT_PAREN);
        result = node_factory_->NewCallExpression(result, std::move(args));
        break;
      }
      case Token::Type::DOT: {
        // Member expression
        Consume(Token::Type::DOT);
        ast::Expression *member = ParseOperand();
        result = node_factory_->NewMemberExpression(result->Position(), result, member);
        break;
        // @ptrCast(*Row, expr)
      }
      case Token::Type::LEFT_BRACKET: {
        // Index expression (i.e., array or map access)
        Consume(Token::Type::LEFT_BRACKET);
        ast::Expression *index = ParseExpression();
        Expect(Token::Type::RIGHT_BRACKET);
        result = node_factory_->NewIndexExpression(result->Position(), result, index);
        break;
      }
      default: {
        break;
      }
    }
  } while (Token::IsCallOrMemberOrIndex(Peek()));

  return result;
}

ast::Expression *Parser::ParseOperand() {
  // Operand = Literal | OperandName | '(' Expression ')'
  // Literal = int_lit | float_lit | 'nil' | 'true' | 'false' | FunctionLiteral
  // OperandName = identifier

  switch (Peek()) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory_->NewNilLiteral(scanner_->CurrentPosition());
    }
    case Token::Type::FALSE:
    case Token::Type::TRUE: {
      const bool bool_val = (Next() == Token::Type::TRUE);
      return node_factory_->NewBoolLiteral(scanner_->CurrentPosition(), bool_val);
    }
    case Token::Type::BUILTIN_IDENTIFIER: {
      // Builtin call expression
      Next();
      ast::Expression *func_name =
          node_factory_->NewIdentifierExpression(scanner_->CurrentPosition(), GetSymbol());
      Consume(Token::Type::LEFT_PAREN);
      util::RegionVector<ast::Expression *> args(Region());
      while (Peek() != Token::Type::RIGHT_PAREN) {
        ast::Expression *arg = ParseExpression();
        args.push_back(arg);
        if (Peek() == Token::Type::COMMA) {
          Next();
        }
      }
      Expect(Token::Type::RIGHT_PAREN);
      return node_factory_->NewBuiltinCallExpression(func_name, std::move(args));
    }
    case Token::Type::IDENTIFIER: {
      Next();
      return node_factory_->NewIdentifierExpression(scanner_->CurrentPosition(), GetSymbol());
    }
    case Token::Type::INTEGER: {
      Next();
      char *end = nullptr;
      int64_t num = std::strtoll(GetSymbol().GetData(), &end, 10);
      return node_factory_->NewIntLiteral(scanner_->CurrentPosition(), num);
    }
    case Token::Type::FLOAT: {
      Next();
      char *end = nullptr;
      float num = std::strtof(GetSymbol().GetData(), &end);
      return node_factory_->NewFloatLiteral(scanner_->CurrentPosition(), num);
    }
    case Token::Type::STRING: {
      Next();
      return node_factory_->NewStringLiteral(scanner_->CurrentPosition(), GetSymbol());
    }
    case Token::Type::FUN: {
      Next();
      return ParseFunctionLiteralExpression();
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      ast::Expression *expr = ParseExpression();
      Expect(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: {
      break;
    }
  }

  // Error
  error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kExpectingExpression);
  Next();
  return node_factory_->NewBadExpression(scanner_->CurrentPosition());
}

ast::Expression *Parser::ParseFunctionLiteralExpression() {
  // FunctionLiteral = Signature FunctionBody ;
  //
  // FunctionBody = Block ;

  // Parse the type
  auto *func_type = ParseFunctionType()->As<ast::FunctionTypeRepr>();

  // Parse the body
  auto *body = ParseBlockStatement()->As<ast::BlockStatement>();

  // Done
  return node_factory_->NewFunctionLiteralExpression(func_type, body);
}

ast::Expression *Parser::ParseType() {
  switch (Peek()) {
    case Token::Type::NIL:
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner_->CurrentPosition();
      return node_factory_->NewIdentifierExpression(position, GetSymbol());
    }
    case Token::Type::MAP: {
      return ParseMapType();
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
      Next();
      return ParseStructType();
    }
    default: {
      break;
    }
  }

  // Error
  error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kExpectingType);

  return nullptr;
}

ast::Expression *Parser::ParseFunctionType() {
  // FuncType = '(' { ParameterList } ')' '->' Type ;
  // ParameterList = { Ident ':' } Type ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::LEFT_PAREN);

  util::RegionVector<ast::FieldDeclaration *> params(Region());
  params.reserve(4);

  while (Peek() != Token::Type::RIGHT_PAREN) {
    const SourcePosition &field_position = scanner_->CurrentPosition();

    ast::Identifier ident;

    ast::Expression *type = nullptr;

    if (Matches(Token::Type::IDENTIFIER)) {
      ident = GetSymbol();
    }

    if (Matches(Token::Type::COLON) || ident.IsEmpty()) {
      type = ParseType();
    } else {
      type = node_factory_->NewIdentifierExpression(field_position, ident);
      ident = ast::Identifier();
    }

    // That's it
    params.push_back(node_factory_->NewFieldDeclaration(field_position, ident, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  ast::Expression *ret = ParseType();

  return node_factory_->NewFunctionType(position, std::move(params), ret);
}

ast::Expression *Parser::ParsePointerType() {
  // PointerTypeRepr = '*' Type ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Expect(Token::Type::STAR);

  ast::Expression *base = ParseType();

  return node_factory_->NewPointerType(position, base);
}

ast::Expression *Parser::ParseArrayType() {
  // ArrayTypeRepr = '[' Length ']' Type ;
  // Length = [ '*' | Expression ] ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::LEFT_BRACKET);

  // If the next token doesn't match a right bracket, it means we have a length
  ast::Expression *len = nullptr;
  if (!Matches(Token::Type::RIGHT_BRACKET)) {
    if (!Matches(Token::Type::STAR)) {
      len = ParseExpression();
    }
    Expect(Token::Type::RIGHT_BRACKET);
  } else {
    error_reporter_->Report(position, sema::ErrorMessages::kMissingArrayLength);
  }

  // Now the type
  ast::Expression *elem_type = ParseType();

  // Done
  return node_factory_->NewArrayType(position, len, elem_type);
}

ast::Expression *Parser::ParseStructType() {
  // StructType = '{' { Ident ':' Type } '}' ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::LEFT_BRACE);

  util::RegionVector<ast::FieldDeclaration *> fields(Region());

  while (Peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);

    const SourcePosition &field_position = scanner_->CurrentPosition();

    // The parameter name
    ast::Identifier name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expression *type = ParseType();

    // That's it
    fields.push_back(node_factory_->NewFieldDeclaration(field_position, name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory_->NewStructType(position, std::move(fields));
}

ast::Expression *Parser::ParseMapType() {
  // MapType = 'map' '[' Expression ']' Expression ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::MAP);

  Expect(Token::Type::LEFT_BRACKET);

  ast::Expression *key_type = ParseType();

  Expect(Token::Type::RIGHT_BRACKET);

  ast::Expression *value_type = ParseType();

  return node_factory_->NewMapType(position, key_type, value_type);
}

}  // namespace tpl::parsing
