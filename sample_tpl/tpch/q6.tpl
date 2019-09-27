struct outputStruct {
  out: Real
}

struct State {
    sum: RealSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggInit(&state.sum)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil { }

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 1 (hashing)
    var out : *outputStruct
    var tvi: TableVectorIterator
    @tableIterInit(&tvi, "lineitem")
    for (@tableIterAdvance(&tvi)) {
        var vpi = @tableIterGetVPI(&tvi)

        var filter: VectorFilterExecutor
        @filterExecInit(&filter, vpi)
        @filterExecGt(&filter, 4, @floatToSql(24.0))       // quantity
        @filterExecGt(&filter, 6, @floatToSql(0.04))       // discount
        @filterExecLt(&filter, 6, @floatToSql(0.06))       // discount
        @filterExecGe(&filter, 10, @dateToSql(1994, 1, 1)) // ship date
        @filterExecGe(&filter, 10, @dateToSql(1995, 1, 1)) // ship date
        @filterExecFinish(&filter)
        @filterExecFree(&filter)

        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            //if (@vpiGetReal(vpi, 4) > 24.0
            //        and @vpiGetReal(vpi, 6) > 0.04
            //        and @vpiGetReal(vpi, 6) < 0.06
            //        and @vpiGetDate(vpi, 10) >= @dateToSql(1994, 1, 1)
            //        and @vpiGetDate(vpi, 10) <= @dateToSql(1995, 1, 1)) {
                var input = @vpiGetReal(vpi, 5) * @vpiGetReal(vpi, 6) // extendedprice * discount
                @aggAdvance(&state.sum, &input)
            //}
        }
    }

    // Pipeline 2 (Output to upper layers)
    out = @ptrCast(*outputStruct, @resultBufferAllocRow(execCtx))
    out.out = @aggResult(&state.sum)
    @resultBufferFinalize(execCtx)
    @tableIterClose(&tvi)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return 37
}
