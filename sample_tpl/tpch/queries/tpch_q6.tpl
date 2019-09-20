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
  var tvi: TableVectorIterator
  var oids : [4]uint32
  oids[0] = 5 // l_quantity
  oids[1] = 6 // l_extendedprice
  oids[2] = 7 // l_discount
  oids[3] = 11 // l_shipdate
  @tableIterInitBind(&tvi, execCtx, "lineitem", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      if (@pciGetDouble(pci, 0) > 24.0 // quantity
          and @pciGetDouble(pci, 2) > 0.04 // discount
          and @pciGetDouble(pci, 2) < 0.06 // discount
          and @pciGetDate(pci, 3) >= @dateToSql(1994, 1, 1) // ship date
          and @pciGetDate(pci, 3) <= @dateToSql(1995, 1, 1)) { // ship date
        var input = @pciGetDouble(pci, 1) * @pciGetDouble(pci, 2) // extendedprice * discount
        @aggAdvance(&state.sum, &input)
      }
    }
  }

  // Pipeline 2 (Output to upper layers)
  var out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
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
