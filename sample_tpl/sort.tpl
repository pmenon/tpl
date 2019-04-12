struct Row {
  a: int32
  b: int32
}

fun compareFn(lhs: *Row, rhs: *Row) -> bool {
  return lhs.a < rhs.a
}

fun main() -> int32 {
  var sorter: Sorter
  @sorterInit(&sorter, &compareFn, @sizeof(row_type))

  for (row in test) {
    var elem = @ptrCast(*Row, @sorterInsert(&sorter))
    elem.a = row.a
    elem.b = row.b
  }

  @sorterSort(&sorter)

  for (row in sorter) {

  }

  return 0
}
