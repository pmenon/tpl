fun main() -> int {
  var a = 1
  var b = 2
  {
    var a = 3
    b = b + a               // b = 2 + 3 = 5
  }
  {
    var b = 721
  }

  b = b + a                 // b = 5 + 1 = 6
  var a = 7
  b = b * a                 // b = 6 * 7 = 42

  return b
}