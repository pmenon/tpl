// Perform:
//
// SELECT col_b, count(col_a) FROM test_1 GROUP BY col_b
//

struct State {
  table: AggregationHashTable
  count: int32
}

struct Agg {
  key: Integer
  count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, vpi: *VectorProjectionIterator) -> bool {
  var key = @vpiGetInt(vpi, 1)
  return @sqlToBool(key == agg.key)
}

fun constructAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
  agg.key = @vpiGetInt(vpi, 1)
  @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
  var input = @vpiGetInt(vpi, 0)
  @aggAdvance(&agg.count, &input)
}

fun pipeline_1(state: *State) -> nil {
  var ht = &state.table
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 1))
      var agg = @ptrCast(*Agg, @aggHTLookup(ht, hash_val, keyCheck, vec))
      if (agg == nil) {
        agg = @ptrCast(*Agg, @aggHTInsert(ht, hash_val))
        constructAgg(agg, vec)
      } else {
        updateAgg(agg, vec)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
  var aht_iter: AHTIterator
  var iter = &aht_iter
  for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
    var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
    state.count = state.count + 1
  }
  @aggHTIterClose(iter)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  state.count = 0

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(&state)

  // Run pipeline 2
  pipeline_2(&state)

  var ret = state.count

  // Cleanup
  tearDownState(&state)

  return ret
}
