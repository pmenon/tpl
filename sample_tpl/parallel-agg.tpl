struct State {
  table: AggregationHashTable
}

struct ThreadState_1 {
  table: AggregationHashTable
}

struct Agg {
  key: Integer
  cs : CountStarAggregate
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
  @aggInit(&agg.cs)
}

fun updateAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  var input = @vpiGetInt(iters[0], 1)
  @aggAdvance(&agg.cs, &input)
}

fun initState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun p1_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun p1_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @aggHTFree(&state.table)
}

fun p1_worker(ctx: *ExecutionContext, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var iters: [1]*VectorProjectionIterator
  var ht: *AggregationHashTable = &state.table

  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetVPI(tvi)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg)
  }
  return
}

fun p1_mergePartitions(qs: *State, table: *AggregationHashTable, iter: *AggOverflowPartIter) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var agg_hash = @aggPartIterGetHash(iter)
    var agg = @ptrCast(*Agg, @aggPartIterGetRow(iter))
  }
}

fun p2_worker(qs: *State, state: *ThreadState_1, table: *AggregationHashTable) -> nil {
  // Something
}

fun main(execCtx: *ExecutionContext) -> int {
  var state: State

  // ---- Init ---- //

  initState(execCtx, &state)

  // ---- Pipeline 1 ---- // 
  
  var tls: ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState_1), p1_worker_initThreadState, p1_worker_tearDownThreadState, execCtx)

  // Parallel Scan
  @iterateTableParallel("test_1", execCtx, &tls, p1_worker)

  // Move thread-local states
  var aht_off: uint32 = 0
  @aggHTMoveParts(&state.table, &tls, aht_off, p1_mergePartitions)

  // ---- Pipeline 2 ---- //

  @aggHTParallelPartScan(&state.table, &state, &tls, p2_worker)

  // ---- Clean Up ---- //

  @tlsFree(&tls)
  tearDownState(execCtx, &state)

  return 0
}
