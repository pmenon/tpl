fun main(ctx: *ExecutionContext) -> int32 {
  var a = @stringToSql("hello")
  var b = @stringToSql(".")
  var c = @stringToSql("there")
  var xx = @concat(ctx, a, b, c)
  if (xx==@stringToSql("hello.there"))   {
    return 0
  }
  return 11
}
