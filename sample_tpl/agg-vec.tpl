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
  state.count = 0
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, iters: [*]*VectorProjectionIterator) -> bool {
  var key = @vpiGetInt(iters[0], 1)
  return @sqlToBool(key == agg.key)
}

fun vecKeyCheck(aggs: [*]*Agg, iters: [*]*VectorProjectionIterator, indexes: [*]uint32, matches: [*]bool, num_elems: uint32) -> nil {
  var vec = iters[0]
  for (var i : uint32 = 0; i < num_elems; i = i + 1) {
    var agg = aggs[i]
    var index = indexes[i]
    @vpiSetPosition(vec, index)
    var key = @vpiGetInt(vec, 1)
    if (key == agg.key) {
      matches[i] = true
    }
  }
}

fun vecHashFn(hashes: [*]uint64, iters: [*]*VectorProjectionIterator) -> nil {
  var vec = iters[0]
  var idx = 0
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    hashes[idx] = @hash(@vpiGetInt(vec, 1))
    idx = idx + 1
  }
}

fun constructAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  // Set key
  agg.key = @vpiGetInt(iters[0], 1)
  // Initialize aggregate
  @aggInit(&agg.count)
}

fun vecUpdateAgg(aggs: [*]*Agg, iters: [*]*VectorProjectionIterator, indexes: [*]uint32, num_elems: uint32) -> nil {
  var vec = iters[0]
  for (var i : uint32 = 0; i < num_elems; i = i + 1) {
    var agg = aggs[i]
    var index = indexes[i]
    @vpiSetPosition(vec, index)
    var input = @vpiGetInt(vec, 0)
    @aggAdvance(&agg.count, &input)
  }
}

fun pipeline_1(state: *State) -> nil {
  var iters: [1]*VectorProjectionIterator

  // The table
  var ht: *AggregationHashTable = &state.table

  // Setup the iterator and iterate
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, vecHashFn, keyCheck, vecKeyCheck, constructAgg, vecUpdateAgg, false)
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
