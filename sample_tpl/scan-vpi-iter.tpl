fun filter_clause0term0(vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(vector_proj, 0, @intToSql(500), tids)
}

fun count(vpi: *VectorProjectionIterator) -> int32 {
    var ret = 0
    if (@vpiIsFiltered(vpi)) {
        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
          ret = ret + 1
        }
    } else {
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
          ret = ret + 1
        }
    }
    @vpiResetFiltered(vpi)
    return ret
}

fun main(execCtx: *ExecutionContext) -> int {
    var ret :int = 0

    var filter: FilterManager
    @filterManagerInit(&filter)
    @filterManagerInsertFilter(&filter, filter_clause0term0)

    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        @filterManagerRunFilters(&filter, vpi)
        ret = ret + count(vpi)
    }

    @filterManagerFree(&filter)
    @tableIterClose(&tvi)
    return ret
}
