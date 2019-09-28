struct Output {
    out: Real
}

// -----------------------------------------------------------------------------
// Query States
// -----------------------------------------------------------------------------

struct State {
    count : int32
    sum   : RealSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggInit(&state.sum)
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil { }

// -----------------------------------------------------------------------------
// Pipeline 1 Thread States
// -----------------------------------------------------------------------------

struct P1_ThreadState {
    ts_sum   : RealSumAggregate
    filter   : FilterManager
    ts_count : int32
}

fun p1_filter(vec: *VectorProjectionIterator) -> int32 {
    var filter: VectorFilterExecutor
    @filterExecInit(&filter, vec)
    @filterExecGt(&filter, 4, @floatToSql(24.0))       // quantity
    @filterExecGt(&filter, 6, @floatToSql(0.04))       // discount
    @filterExecLt(&filter, 6, @floatToSql(0.06))       // discount
    @filterExecGe(&filter, 10, @dateToSql(1994, 1, 1)) // ship date
    @filterExecLe(&filter, 10, @dateToSql(1995, 1, 1)) // ship date
    @filterExecFinish(&filter)
    @filterExecFree(&filter)
    return 0
}

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    ts.ts_count = 0
    @aggInit(&ts.ts_sum)
    @filterManagerInit(&ts.filter)
    @filterManagerInsertFilter(&ts.filter, p1_filter)
    @filterManagerFinalize(&ts.filter)
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil { }

// -----------------------------------------------------------------------------
// Pipeline 1
// -----------------------------------------------------------------------------

fun p1_worker(state: *State, ts: *P1_ThreadState, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)

    // Filter
    @filtersRun(&ts.filter, vec)

    // Aggregate
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var input = @vpiGetReal(vec, 5) * @vpiGetReal(vec, 6) // extendedprice * discount
      @aggAdvance(&ts.ts_sum, &input)
      ts.ts_count = ts.ts_count + 1
    }
  }
}

fun p1_mergeAggregates(qs: *State, ts: *P1_ThreadState) -> nil {
  @aggMerge(&qs.sum, &ts.ts_sum)
  qs.count = qs.count + ts.ts_count
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P1_ThreadState), p1_initThreadState, p1_tearDownThreadState, execCtx)

    // Scan lineitem
    @iterateTableParallel("lineitem", state, &tls, p1_worker)

    // Merge results
    @tlsIterate(&tls, state, p1_mergeAggregates)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 2
// -----------------------------------------------------------------------------

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var out = @ptrCast(*Output, @resultBufferAllocRow(execCtx))
    out.out = @aggResult(&state.sum)
    @resultBufferFinalize(execCtx)
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
