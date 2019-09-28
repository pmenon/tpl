struct Output {
  out: Real
}

struct State {
    sum   : RealSumAggregate
    count : uint32 // debug
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggInit(&state.sum)
    state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil { }

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "lineitem"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)

        var filter: VectorFilterExecutor
        @filterExecInit(&filter, vpi)
        @filterExecGt(&filter, 4, @floatToSql(24.0))       // quantity
        @filterExecGt(&filter, 6, @floatToSql(0.04))       // discount
        @filterExecLt(&filter, 6, @floatToSql(0.06))       // discount
        @filterExecGe(&filter, 10, @dateToSql(1994, 1, 1)) // ship date
        @filterExecLe(&filter, 10, @dateToSql(1995, 1, 1)) // ship date
        @filterExecFinish(&filter)
        @filterExecFree(&filter)

        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
            state.count = state.count + 1
            var input = @vpiGetReal(vpi, 5) * @vpiGetReal(vpi, 6) // extendedprice * discount
            @aggAdvance(&state.sum, &input)
        }
    }
    @tableIterClose(&tvi)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var out = @ptrCast(*Output, @resultBufferAllocRow(execCtx))
    out.out = @aggResult(&state.sum)
    @resultBufferFinalize(execCtx)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)

    return state.count
}
