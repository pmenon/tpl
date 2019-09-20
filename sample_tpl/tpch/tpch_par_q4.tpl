struct OutputStruct {
  o_orderpriority: StringVal
  order_count: Integer
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


struct JoinBuildRow {
  o_orderkey: Integer
  o_orderpriority: StringVal
  match_flag: bool
}

// Input & Output of the aggregator
struct AggPayload {
  o_orderpriority: StringVal
  order_count: CountStarAggregate
}

// Input & output of the sorter
struct SorterRow {
  o_orderpriority: StringVal
  order_count: Integer
}

struct State {
  join_table: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
  count: int32
}

struct ThreadState1 {
  ts_join_table : JoinHashTable
  filter: FilterManager
  ts_count: int32
}

struct ThreadState2 {
  ts_agg_table: AggregationHashTable
  filter: FilterManager
  ts_count: int32
}

struct ThreadState3 {
  ts_sorter : Sorter
  ts_count: int32
}



// Check that two join keys are equal
fun checkJoinKey(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build_row: *JoinBuildRow) -> bool {
  // l_orderkey == o_orderkey
  return @sqlToBool(@vpiGetInt(probe, 0) == build_row.o_orderkey)
}

// Check that the aggregate key already exists
fun checkAggKey(agg: *AggPayload, build_row: *JoinBuildRow) -> bool {
  return @sqlToBool(agg.o_orderpriority == build_row.o_orderpriority)
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
  return @sqlToBool(agg_payload1.o_orderpriority == agg_payload2.o_orderpriority)
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.o_orderpriority < rhs.o_orderpriority) {
    return -1
  } else if (lhs.o_orderpriority > lhs.o_orderpriority) {
    return 1
  }
  return 0
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
  var lo = @dateToSql(1993, 7, 1)
  var hi = @dateToSql(1993, 10, 1)
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    // 1993-07-01 <= o_orderdate <= 1993-10-01
    @vpiMatch(vec, @vpiGetDate(vec, 4) >= lo and @vpiGetDate(vec, 4) <= hi)
  }
  @vpiResetFiltered(vec)
  return 0
}

fun p2Filter2(vec: *VectorProjectionIterator) -> int32 {
  var lo = @dateToSql(1993, 7, 1)
  var hi = @dateToSql(1993, 10, 1)
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    // if l_commitdate < l_receiptdate
    @vpiMatch(vec, @vpiGetDate(vec, 11) < @vpiGetDate(vec, 12))
  }
  @vpiResetFiltered(vec)
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTFree(&state.agg_table)
  @sorterFree(&state.sorter)
  @joinHTFree(&state.join_table)
}

fun initTheadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p1Filter1)
  @filterManagerFinalize(&ts.filter)
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
  ts.ts_count = 0
}

fun teardownThreadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @filterManagerFree(&ts.filter)
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p2Filter2)
  @filterManagerFinalize(&ts.filter)
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  ts.ts_count = 0
}

fun teardownThreadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @filterManagerFree(&ts.filter)
  @aggHTFree(&ts.ts_agg_table)
}

fun initTheadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  ts.ts_count = 0
}

fun teardownThreadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  @sorterFree(&ts.ts_sorter)
}

fun gatherCounters1(qs: *State, ts: *ThreadState1) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun gatherCounters2(qs: *State, ts: *ThreadState2) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun gatherCounters3(qs: *State, ts: *ThreadState3) -> nil {
  qs.count = 517// qs.count + ts.ts_count
}

// Pipeline 1 (Join Build)
fun worker1(state: *State, ts: *ThreadState1, o_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(o_tvi)) {
    var vec = @tableIterGetVPI(o_tvi)
    // Run Filter
    @filtersRun(&ts.filter, vec)
    // Insert into JHT
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // o_orderkey
      var build_row = @ptrCast(*JoinBuildRow, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
      build_row.o_orderpriority = @vpiGetVarlen(vec, 5) // o_orderpriority
      build_row.match_flag = false
      ts.ts_count = ts.ts_count + 1
    }
  }
}

// Pipeline 2 (Join Probe up to Agg)
fun worker2(state: *State, ts: *ThreadState2, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    // Run Filter
    @filtersRun(&ts.filter, vec)
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      // Step 2: Probe Join Hash Table
      var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
      var join_iter: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table, &join_iter, hash_val); @htEntryIterHasNext(&join_iter, checkJoinKey, state, vec);) {
        var build_row = @ptrCast(*JoinBuildRow, @htEntryIterGetRow(&join_iter))
        // match each row once
        if (!build_row.match_flag) {
          build_row.match_flag = true
          // Step 3: Build Agg Hash Table
          var agg_hash_val = @hash(build_row.o_orderpriority)
          var agg = @ptrCast(*AggPayload, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey, build_row))
          if (agg == nil) {
            agg = @ptrCast(*AggPayload, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
            agg.o_orderpriority = build_row.o_orderpriority
            @aggInit(&agg.order_count)
            ts.ts_count = ts.ts_count + 1
          }
          @aggAdvance(&agg.order_count, &build_row.o_orderpriority)
        }
      }
    }
  }
}

fun mergerPartitions2(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload, @aggHTInsert(agg_table, partial_hash))
      agg_payload.o_orderpriority = partial.o_orderpriority
      @aggInit(&agg_payload.order_count)
    }
    @aggMerge(&agg_payload.order_count, &partial.order_count)
  }
}

// Pipeline 3 (Sort)
fun worker3(state: *State, ts: *ThreadState3, agg_table: *AggregationHashTable) -> nil {
  var aht_iter: AHTIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg = @ptrCast(*AggPayload, @aggHTIterGetRow(&aht_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
    sorter_row.o_orderpriority = agg.o_orderpriority
    sorter_row.order_count = @aggResult(&agg.order_count)
    ts.ts_count = ts.ts_count + 1
  }
  @aggHTIterClose(&aht_iter)
}

// Pipeline 4 (Output)
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    var out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.o_orderpriority = sorter_row.o_orderpriority
    out.order_count = sorter_row.order_count
    state.count = state.count + 1
  }
  @sorterIterClose(&sort_iter)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  // set up state
  setUpState(execCtx, &state)

  // Pipeline 1
  var tls : ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState1), initTheadState1, teardownThreadState1, execCtx)
  // parallel scan + join build
  @iterateTableParallel("orders", &state, &tls, worker1)
  // Parallel Build
  var off: uint32 = 0
  @joinHTBuildParallel(&state.join_table, &tls, off)

  // Pipeline 2
  @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
  // Parallel scan + join probe + agg build
  @iterateTableParallel("lineitem", &state, &tls, worker2)
  // Move thread-local states
  var aht_off: uint32 = 0
  @aggHTMoveParts(&state.agg_table, &tls, aht_off, mergerPartitions2)

  // Pipeline 3
  @tlsReset(&tls, @sizeOf(ThreadState3), initTheadState3, teardownThreadState3, execCtx)
  // Scan AHT and fill up sorters
  @aggHTParallelPartScan(&state.agg_table, &state, &tls, worker3)
  @tlsIterate(&tls, &state, gatherCounters3)
  var sorter_off : uint32 = 0
  @sorterSortParallel(&state.sorter, &tls, sorter_off)

  // Pipeline 4
  pipeline4(execCtx, &state)
  @outputFinalize(execCtx)

  return state.count
}
