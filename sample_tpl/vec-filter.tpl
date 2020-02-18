fun pipeline1_filter_clause0term0(vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(vector_proj, 0, @intToSql(2000), tids)
}

fun main(execCtx: *ExecutionContext) -> int {
  var ret: int = 0

  var filter : FilterManager
  @filterManagerInit(&filter)
  @filterManagerInsertFilter(&filter, pipeline1_filter_clause0term0)

  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi);) {
    // Get the current vector projection
    var vpi = @tableIterGetVPI(&tvi)

    // Filter it
    @filterManagerRunFilters(&filter, vpi)

    // Count survivors
    ret = ret + @vpiSelectedRowCount(vpi)
  }
  @tableIterClose(&tvi)
  @filterManagerFree(&filter)

  return ret
}
