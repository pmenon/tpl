struct State {
  table: AggregationHashTable
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

fun keyCheck(v: *VectorProjectionIterator, agg: *Agg) -> bool {
  var key = @vpiGetInt(v, 0)
  return @sqlToBool(key == agg.key)
}

fun constructAgg(vpi: *VectorProjectionIterator, agg: *Agg) -> nil {
  @aggInit(&agg.count)
}

fun updateAgg(vpi: *VectorProjectionIterator, agg: *Agg) -> nil {
  var input = @vpiGetInt(vpi, 0)
  @aggAdvance(&agg.count, &input)
}

fun pipeline_1(state: *State) -> nil {
  var ht: *AggregationHashTable = &state.table

  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0))
      var agg = @ptrCast(*Agg, @aggHTLookup(ht, hash_val, keyCheck, vec))
      if (agg == nil) {
        agg = @ptrCast(*Agg, @aggHTInsert(ht, hash_val))
        constructAgg(vec, agg)
      } else {
        updateAgg(vec, agg)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var iter = &agg_ht_iter
  for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
    var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter)) 
  }
  @aggHTIterClose(iter)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(&state)

  // Run pipeline 2
  pipeline_2(&state)

  // Cleanup
  tearDownState(&state)

  return 0
}
