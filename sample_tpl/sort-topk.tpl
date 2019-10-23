struct State {
    sorter : Sorter
    count  : uint32
}

struct Row {
    a: Integer
    b: Integer
}

fun compareFn(lhs: *Row, rhs: *Row) -> int32 {
    if (lhs.a < rhs.a) {
        return -1
    } else {
        return 1
    }
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(Row))
    state.count = 0
}

fun tearDownState(state: *State) -> nil {
    @sorterFree(&state.sorter)
}

fun pipeline1_filter_clause0term0(vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    @filterLt(vector_proj, 0, @intToSql(2000), tids)
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var sorter = &state.sorter

    // Setup filter
    var filter : FilterManager
    @filterManagerInit(&filter)
    @filterManagerInsertFilter(&filter, pipeline1_filter_clause0term0)
    @filterManagerFinalize(&filter)

    var tvi: TableVectorIterator
    var top_k: uint32 = 11
    for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)

        // Filter
        @filterManagerRunFilters(&filter, vpi)

        // Insert into sorter
        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
            var row = @ptrCast(*Row, @sorterInsertTopK(sorter, top_k))
            row.a = @vpiGetInt(vpi, 0)
            row.b = @vpiGetInt(vpi, 1)
            @sorterInsertTopKFinish(sorter, top_k)
        }
        @vpiResetFiltered(vpi)
    }
    @tableIterClose(&tvi)

    // Sort
    @sorterSort(&state.sorter)

    // Cleanup
    @filterManagerFree(&filter)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> int32 {
    var ret = 0
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var row = @ptrCast(*Row, @sorterIterGetRow(&sort_iter))
        state.count = state.count + 1
    }
    @sorterIterClose(&sort_iter)
    return ret
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    var ret = state.count
    tearDownState(&state)

    return ret
}
