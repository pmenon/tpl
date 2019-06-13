struct State {
  table: AggregationHashTable
  count: int32
}

struct Agg {
  key: Integer
  cs : CountStarAggregate
  c  : CountAggregate
  sum: IntegerSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
  state.count = 0
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, iters: [*]*VectorProjectionIterator) -> bool {
  var key = @vpiGetInt(iters[0], 1)
  return @sqlToBool(key == agg.key)
}

fun hashFn(iters: [*]*VectorProjectionIterator) -> uint64 {
  return @hash(@vpiGetInt(iters[0], 1))
}

fun constructAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  agg.key = @vpiGetInt(iters[0], 1)
  @aggInit(&agg.cs, &agg.c, &agg.sum)
}

fun updateAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  var input = @vpiGetInt(iters[0], 1)
  @aggAdvance(&agg.c, &input)
  @aggAdvance(&agg.cs, &input)
  @aggAdvance(&agg.sum, &input)
}

fun pipeline_1(state: *State) -> nil {
  var iters: [1]*VectorProjectionIterator

  // The table
  var ht: *AggregationHashTable = &state.table

  // Setup the iterator and iterate
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    @filterLt(vec, 0, 5000)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg, false)
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
