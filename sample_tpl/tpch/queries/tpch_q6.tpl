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
  sum: RealSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
}


fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 1 (hashing)
  var out : *outputStruct
  var tvi: TableVectorIterator
  @tableIterInit(&tvi, "lineitem")
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      if (@vpiGetReal(vpi, 4) > 24.0 // quantity
          and @vpiGetReal(vpi, 6) > 0.04 // discount
          and @vpiGetReal(vpi, 6) < 0.06 // discount
          and @vpiGetDate(vpi, 10) >= @dateToSql(1994, 1, 1) // ship date
          and @vpiGetDate(vpi, 10) <= @dateToSql(1995, 1, 1)) { // ship date
        var input = @vpiGetReal(vpi, 5) * @vpiGetReal(vpi, 6) // extendedprice * discount
        @aggAdvance(&state.sum, &input)
      }
    }
  }

  // Pipeline 2 (Output to upper layers)
  out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
  out.out = @aggResult(&state.sum)
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  setUpState(execCtx, &state)
  execQuery(execCtx, &state)
  teardownState(execCtx, &state)
  return 37
}
