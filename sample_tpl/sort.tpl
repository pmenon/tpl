struct Row {
  a: int32
  b: int32
}

fun compareFn(lhs: *Row, rhs: *Row) -> bool {
  return lhs.a < rhs.a
}

fun main() -> int32 {
  var alloc: RegionAlloc
  @regionInit(&alloc)

  var sorter: Sorter
  @sorterInit(&sorter, &alloc, compareFn, @sizeOf(Row))

  for (row in test_1) {
    var elem: *Row = @sorterInsert(&sorter)
    elem.a = row.colA
    elem.b = row.colB
  }

  @sorterSort(&sorter)

  @sorterFree(&sorter)
  @regionFree(&alloc)

  return 0
}
