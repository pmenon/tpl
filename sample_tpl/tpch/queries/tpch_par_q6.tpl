struct outputStruct {
  out: Real
}

struct DebugOutputStruct {
  d1 : Integer
  d2 : Integer
  d3 : Integer
  d4 : Integer
  d5 : Integer
  d6 : Integer
  d7 : Integer
  d8 : Integer
  d9 : Integer
  d10 : Integer
}

struct State {
  count: int32
  sum: RealSumAggregate
}

struct ThreadState1 {
  ts_sum: RealSumAggregate
  filter: FilterManager
  ts_count : int32
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
  var qty = @floatToSql(24.0)
  var discount_lo = @floatToSql(0.04)
  var discount_hi = @floatToSql(0.06)
  var shipdate_lo = @dateToSql(1994, 1, 1)
  var shipdate_hi = @dateToSql(1995, 1, 1)

  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    @vpiMatch(vec, (@vpiGetReal(vec, 4) > qty) // l_quantity
                and (@vpiGetReal(vec, 6) > discount_lo) // l_discount
                and (@vpiGetReal(vec, 6) < discount_hi) // l_discount
                and (@vpiGetDate(vec, 10) >= shipdate_lo) // l_shipdate
                and (@vpiGetDate(vec, 10) <= shipdate_hi)) // l_shipdate
  }
  @vpiResetFiltered(vec)
  return 0
}

fun initTheadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  ts.ts_count = 0
  @aggInit(&ts.ts_sum)
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p1Filter1)
  @filterManagerFinalize(&ts.filter)
}

fun teardownThreadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
}

fun worker1(state: *State, ts: *ThreadState1, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    @filtersRun(&ts.filter, vec)
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var input = @vpiGetReal(vec, 5) * @vpiGetReal(vec, 6) // extendedprice * discount
      @aggAdvance(&ts.ts_sum, &input)
      ts.ts_count = ts.ts_count + 1
    }
  }
}

fun gatherAgg(qs: *State, ts: *ThreadState1) -> nil {
  @aggMerge(&qs.sum, &ts.ts_sum)
  qs.count = qs.count + ts.ts_count
}



fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  setUpState(execCtx, &state)
  var tls : ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))

  // Pipeline 1
  @tlsReset(&tls, @sizeOf(ThreadState1), initTheadState1, teardownThreadState1, execCtx)
  @iterateTableParallel("lineitem", &state, &tls, worker1)
  @tlsIterate(&tls, &state, gatherAgg)

  var out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
  out.out = @aggResult(&state.sum)
  @outputFinalize(execCtx)

  return state.count
}
