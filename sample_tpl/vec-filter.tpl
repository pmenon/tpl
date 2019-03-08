fun main() -> int {
  var count = 0
  for (vec in test_1@[batch=2048]) {
    count = count + tpl_filter_gt(vec, "colA", 500)
  }
  return count
}
