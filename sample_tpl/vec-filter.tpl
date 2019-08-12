
fun main(execCtx: *ExecutionContext) -> int {
  var ret: int = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi);) {
    // Get the current vector projection
    var vpi = @tableIterGetVPI(&tvi)

    // Filter it
    var filter: VectorFilterExecutor
    @filterExecInit(&filter, vpi)
    @filterExecLt(&filter, 0, @intToSql(500))
    @filterExecFinish(&filter)
    @filterExecFree(&filter)

    // Sum
    ret = ret + @vpiSelectedRowCount(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
