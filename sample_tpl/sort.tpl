struct State {
  alloc: RegionAlloc
  sorter: Sorter
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

fun setUpState(state: *State) -> nil {
  @regionInit(&state.alloc)
  @sorterInit(&state.sorter, &state.alloc, compareFn, @sizeOf(Row))
}

fun pipeline_1(state: *State) -> nil {
  var sorter = &state.sorter
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var row = @ptrCast(*Row, @sorterInsert(sorter))
      row.a = @vpiGetInt(vpi, 0)
      row.b = @vpiGetInt(vpi, 1)
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter);
       @sorterIterHasNext(&sort_iter);
       @sorterIterNext(&sort_iter)) {
    var row = @ptrCast(*Row, @sorterIterGetRow(&sort_iter))
  }
  @sorterIterClose(&sort_iter)
}

fun tearDownState(state: *State) -> nil {
  @sorterFree(&state.sorter)
  @regionFree(&state.alloc)
}

fun main() -> int32 {
  var state: State

  // Initialize
  setUpState(&state)

  // Pipeline 1
  pipeline_1(&state)

  // Pipeline 1 end
  @sorterSort(&state.sorter)

  // Pipeline 2
  pipeline_2(&state)

  // Cleanup
  tearDownState(&state)

  return 0
}
