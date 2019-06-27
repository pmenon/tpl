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

fun batchKeyCheck(aggs: [*]*Agg, iters: [*]*VectorProjectionIterator, indexes: [*]uint32, matches: [*]bool, num_elems: uint32) -> nil {
  for (var i : uint32 = 0; i < num_elems; i = i + 1) {
    var agg = aggs[i]
    var index = indexes[i]
    @vpiSetPosition(iters[0], index)
    var key = @vpiGetInt(iters[0], 1)
    if (key != agg.key) {
      matches[i] = false
    }
  }
}

fun hashFn(iters: [*]*VectorProjectionIterator) -> uint64 {
  return @hash(@vpiGetInt(iters[0], 1))
}

fun batchHashFn(iters: [*]*VectorProjectionIterator, hashes: [*]uint64) -> nil {
  var idx = 0
  for (; @vpiHasNext(iters[0]); @vpiAdvance(iters[0])) {
    hashes[idx] = @hash(@vpiGetInt(iters[0], 1))
    idx = idx + 1
  }
}

fun constructAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  // Set key
  agg.key = @vpiGetInt(iters[0], 1)
  // Initialize aggregate
  @aggInit(&agg.count)
}

fun batchConstructAgg(aggs: [*]*Agg, iters: [*]*VectorProjectionIterator, indexes: [*]uint32, num_elems: uint32) -> nil {
  //for (var i : uint32 = 0; i < num_elems; i = i + 1) {
    // var index = indexes[i]
    // var agg = aggs[i]
    // @vpiSetPosition(iters[0], index)
    // Set key
    // agg.key = @vpiGetInt(iters[0], 1)
    // Initialize aggregate
    // @aggInit(&agg.count)
  //}
}

fun updateAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  var input = @vpiGetInt(iters[0], 0)
  @aggAdvance(&agg.count, &input)
}

fun batchUpdateAgg(aggs: [*]*Agg, iters: [*]*VectorProjectionIterator, indexes: [*]uint32, num_elems: uint32) -> nil {
  for (var i : uint32 = 0; i < num_elems; i = i + 1) {
    //var index = indexes[i]
    //var agg = aggs[i]
    //@vpiSetPosition(iters[0], index)
    //var input = @vpiGetInt(iters[0], 0)
    //@aggAdvance(&agg.count, &input)
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
    //@aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg, false)
    @aggHTProcessBatchArray(ht, &iters, batchHashFn, batchKeyCheck, constructAgg, batchUpdateAgg, keyCheck, false)
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
  // pipeline_2(&state)

  var ret = state.count

  // Cleanup
  tearDownState(&state)

  return ret
}
