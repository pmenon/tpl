struct OutputStruct {
  c_name : StringVal
  c_custkey : Integer
  o_orderkey : Integer
  o_orderdate : Date
  o_totalprice : Real
  sum_quantity : Real
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
  count: int32 // Debug
  join_table1 : JoinHashTable
  join_table2 : JoinHashTable
  join_table3 : JoinHashTable
  agg_table1 : AggregationHashTable
  agg_table2 : AggregationHashTable
  sorter : Sorter
}

struct ThreadState1 {
  ts_agg_table : AggregationHashTable
  ts_count: int32
}

struct ThreadState2 {
  ts_join_table : JoinHashTable
  ts_count: int32
}

struct ThreadState3 {
  ts_join_table : JoinHashTable
  ts_count: int32
}

struct ThreadState4 {
  ts_join_table : JoinHashTable
  ts_count: int32
}

struct ThreadState5 {
  ts_agg_table : AggregationHashTable
  ts_count: int32
}

struct ThreadState6 {
  ts_sorter: Sorter
  ts_count: int32
}

struct JoinRow1 {
  l_orderkey : Integer
}

struct JoinRow2 {
  c_custkey : Integer
  c_name : StringVal
}

struct JoinRow3 {
  c_custkey : Integer
  c_name : StringVal
  o_orderkey : Integer
  o_orderdate : Date
  o_totalprice : Real
}

struct AggValues1 {
  l_orderkey : Integer
  sum_quantity : Real
}

struct AggPayload1 {
  l_orderkey : Integer
  sum_quantity : RealSumAggregate
}

struct AggValues2 {
  c_name : StringVal
  c_custkey : Integer
  o_orderkey : Integer
  o_orderdate : Date
  o_totalprice : Real
  sum_quantity : Real
}

struct AggPayload2 {
  c_name : StringVal
  c_custkey : Integer
  o_orderkey : Integer
  o_orderdate : Date
  o_totalprice : Real
  sum_quantity : RealSumAggregate
}

struct SorterRow {
  c_name : StringVal
  c_custkey : Integer
  o_orderkey : Integer
  o_orderdate : Date
  o_totalprice : Real
  sum_quantity : Real
}


fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
  // o_orderkey == l_orderkey
  if (@vpiGetInt(probe, 0) != build.l_orderkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
  // o_custkey != c_custkey
  if (@vpiGetInt(probe, 1) != build.c_custkey) {
    return false
  }
  return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow3) -> bool {
  // l_orderkey == o_orderkey
  if (@vpiGetInt(probe, 0) != build.o_orderkey) {
    return false
  }
  return true
}

fun checkAggKey1(payload: *AggPayload1, row: *AggValues1) -> bool {
  if (payload.l_orderkey != row.l_orderkey) {
    return false
  }
  return true
}

fun aggKeyCheckPartial1(agg_payload1: *AggPayload1, agg_payload2: *AggPayload1) -> bool {
  return @sqlToBool(agg_payload1.l_orderkey == agg_payload2.l_orderkey)
}

fun checkAggKey2(payload: *AggPayload2, row: *AggValues2) -> bool {
  if (payload.c_custkey != row.c_custkey) {
    return false
  }
  if (payload.o_orderkey != row.o_orderkey) {
    return false
  }
  if (payload.o_orderdate != row.o_orderdate) {
    return false
  }
  if (payload.o_totalprice != row.o_totalprice) {
    return false
  }
  if (payload.c_name != row.c_name) {
    return false
  }
  return true
}

fun aggKeyCheckPartial2(agg_payload1: *AggPayload2, agg_payload2: *AggPayload2) -> bool {
  return (agg_payload1.c_custkey == agg_payload2.c_custkey)
     and (agg_payload1.o_orderkey == agg_payload2.o_orderkey)
     and (agg_payload1.o_orderdate == agg_payload2.o_orderdate)
     and (agg_payload1.o_totalprice == agg_payload2.o_totalprice)
     and (agg_payload1.c_name == agg_payload2.c_name)
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.o_totalprice < rhs.o_totalprice) {
    return 1 // desc
  }
  if (lhs.o_totalprice > rhs.o_totalprice) {
    return -1 // desc
  }
  if (lhs.o_orderdate < rhs.o_orderdate) {
    return -1
  }
  if (lhs.o_orderdate > rhs.o_orderdate) {
    return 1
  }
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
  @aggHTInit(&state.agg_table1, @execCtxGetMem(execCtx), @sizeOf(AggPayload1))
  @aggHTInit(&state.agg_table2, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @joinHTFree(&state.join_table3)
  @aggHTFree(&state.agg_table1)
  @aggHTFree(&state.agg_table2)
  @sorterFree(&state.sorter)
}


fun initTheadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  ts.ts_count = 0
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload1))
}

fun teardownThreadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @aggHTFree(&ts.ts_agg_table)
}

fun initTheadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
}

fun teardownThreadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
}

fun teardownThreadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState4(execCtx: *ExecutionContext, ts: *ThreadState4) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
}

fun teardownThreadState4(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState5(execCtx: *ExecutionContext, ts: *ThreadState5) -> nil {
  ts.ts_count = 0
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
}

fun teardownThreadState5(execCtx: *ExecutionContext, ts: *ThreadState5) -> nil {
  @aggHTFree(&ts.ts_agg_table)
}

fun initTheadState6(execCtx: *ExecutionContext, ts: *ThreadState6) -> nil {
  ts.ts_count = 0
  @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownThreadState6(execCtx: *ExecutionContext, ts: *ThreadState6) -> nil {
  @sorterFree(&ts.ts_sorter)
}

fun gatherCounters1(qs: *State, ts: *ThreadState1) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun gatherCounters2(qs: *State, ts: *ThreadState2) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun gatherCounters3(qs: *State, ts: *ThreadState3) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun gatherCounters4(qs: *State, ts: *ThreadState4) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun mergerPartitions1(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload1, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload1, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial1, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload1, @aggHTInsert(agg_table, partial_hash))
      agg_payload.l_orderkey = partial.l_orderkey
      @aggInit(&agg_payload.sum_quantity)
    }
    @aggMerge(&agg_payload.sum_quantity, &partial.sum_quantity)
  }
}

fun mergerPartitions5(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload2, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial2, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload2, @aggHTInsert(agg_table, partial_hash))
      agg_payload.c_name = partial.c_name
      agg_payload.c_custkey = partial.c_custkey
      agg_payload.o_orderkey = partial.o_orderkey
      agg_payload.o_orderdate = partial.o_orderdate
      agg_payload.o_totalprice = partial.o_totalprice
      @aggInit(&agg_payload.sum_quantity)
    }
    @aggMerge(&agg_payload.sum_quantity, &partial.sum_quantity)
  }
}


// Scan lineitem, build AHT1
fun worker1(state: *State, ts: *ThreadState1, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var agg_input : AggValues1
      agg_input.l_orderkey = @vpiGetInt(vec, 0) // l_orderkey
      agg_input.sum_quantity = @vpiGetReal(vec, 4) // l_quantity
      var agg_hash_val = @hash(agg_input.l_orderkey)
      var agg_payload = @ptrCast(*AggPayload1, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey1, &agg_input))
      if (agg_payload == nil) {
        agg_payload = @ptrCast(*AggPayload1, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
        agg_payload.l_orderkey = agg_input.l_orderkey
        @aggInit(&agg_payload.sum_quantity)
      }
      @aggAdvance(&agg_payload.sum_quantity, &agg_input.sum_quantity)
    }
  }
}

// Scan AHT1, Build JHT1
fun worker2(state: *State, ts: *ThreadState2, agg_table: *AggregationHashTable) -> nil {
  var aht_iter: AHTIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg_payload = @ptrCast(*AggPayload1, @aggHTIterGetRow(&aht_iter))
    if (@aggResult(&agg_payload.sum_quantity) > 300.0) {
      var hash_val = @hash(agg_payload.l_orderkey)
      var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row1.l_orderkey = agg_payload.l_orderkey
      //ts.ts_count = ts.ts_count + 1
    }
  }
  @aggHTIterClose(&aht_iter)
}

// Scan customer, build JHT2
fun worker3(state: *State, ts: *ThreadState3, c_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(c_tvi)) {
    var vec = @tableIterGetVPI(c_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // c_custkey
      var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row2.c_custkey = @vpiGetInt(vec, 0) // c_custkey
      build_row2.c_name = @vpiGetVarlen(vec, 1) // c_name
      //ts.ts_count = ts.ts_count + 1
    }
  }
}

// Scan orders, probe JHT1, probe JHT2, build JHT3
fun worker4(state: *State, ts: *ThreadState4, o_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(o_tvi)) {
    var vec = @tableIterGetVPI(o_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // o_orderkey
      var hti: HashTableEntryIterator
      // Semi-Join with JHT1
      @joinHTLookup(&state.join_table1, &hti, hash_val)
      if (@htEntryIterHasNext(&hti, checkJoinKey1, state, vec)) {
        var hash_val2 = @hash(@vpiGetInt(vec, 1)) // o_custkey
        var hti2: HashTableEntryIterator
        for (@joinHTLookup(&state.join_table2, &hti2, hash_val2); @htEntryIterHasNext(&hti2, checkJoinKey2, state, vec);) {
          var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti2))

          // Build JHT3
          var hash_val3 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
          var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&ts.ts_join_table, hash_val3))
          build_row3.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
          build_row3.o_orderdate = @vpiGetDate(vec, 4) // o_orderdate
          build_row3.o_totalprice = @vpiGetReal(vec, 3) // o_totalprice
          build_row3.c_custkey = join_row2.c_custkey
          build_row3.c_name = join_row2.c_name
          //ts.ts_count = ts.ts_count + 1
        }
      }
    }
  }
}

// Scan lineitem, probe JHT3, build AHT2
fun worker5(state: *State, ts: *ThreadState5, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table3, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey3, state, vec);) {
        var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti))

        // Build AHT
        var agg_input : AggValues2
        agg_input.c_name = join_row3.c_name
        agg_input.c_custkey = join_row3.c_custkey
        agg_input.o_orderkey = join_row3.o_orderkey
        agg_input.o_orderdate = join_row3.o_orderdate
        agg_input.o_totalprice = join_row3.o_totalprice
        agg_input.sum_quantity = @vpiGetReal(vec, 4) // l_quantity
        var agg_hash_val = @hash(agg_input.c_name, agg_input.c_custkey, agg_input.o_orderkey, agg_input.o_totalprice)
        var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey2, &agg_input))
        if (agg_payload == nil) {
          agg_payload = @ptrCast(*AggPayload2, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
          agg_payload.c_name = agg_input.c_name
          agg_payload.c_custkey = agg_input.c_custkey
          agg_payload.o_orderkey = agg_input.o_orderkey
          agg_payload.o_orderdate = agg_input.o_orderdate
          agg_payload.o_totalprice = agg_input.o_totalprice
          @aggInit(&agg_payload.sum_quantity)
          //ts.ts_count = ts.ts_count + 1
        }
        @aggAdvance(&agg_payload.sum_quantity, &agg_input.sum_quantity)
      }
    }
  }
}

// Scan AHT2, sort
fun worker6(state: *State, ts: *ThreadState6, agg_table: *AggregationHashTable) -> nil {
  var aht_iter: AHTIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg_payload = @ptrCast(*AggPayload2, @aggHTIterGetRow(&aht_iter))
    // TODO(Amadou): Use SorterTopK.
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
    sorter_row.c_name = agg_payload.c_name
    sorter_row.c_custkey = agg_payload.c_custkey
    sorter_row.o_orderkey = agg_payload.o_orderkey
    sorter_row.o_orderdate = agg_payload.o_orderdate
    sorter_row.o_totalprice = agg_payload.o_totalprice
    sorter_row.sum_quantity = @aggResult(&agg_payload.sum_quantity)
    //ts.ts_count = ts.ts_count + 1
  }
  @aggHTIterClose(&aht_iter)
}


// Iterate through sorter, output
fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  // TODO(Amadou): Use sorter SorterTopK
  var limit = 0
  for (@sorterIterInit(&sort_iter, &state.sorter); limit < 100 and @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    var out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    out.c_name = sorter_row.c_name
    out.c_custkey = sorter_row.c_custkey
    out.o_orderkey = sorter_row.o_orderkey
    out.o_orderdate = sorter_row.o_orderdate
    out.o_totalprice = sorter_row.o_totalprice
    out.sum_quantity = sorter_row.sum_quantity
    limit = limit + 1
  }
  @outputFinalize(execCtx)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  // set up state
  setUpState(execCtx, &state)
  var off: uint32 = 0
  var tls : ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))

  // Pipeline 1
  @tlsReset(&tls, @sizeOf(ThreadState1), initTheadState1, teardownThreadState1, execCtx)
  @iterateTableParallel("lineitem", &state, &tls, worker1)
  @aggHTMoveParts(&state.agg_table1, &tls, off, mergerPartitions1)
  //@tlsIterate(&tls, &state, gatherCounters1)

  // Pipeline 2
  @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
  @aggHTParallelPartScan(&state.agg_table1, &state, &tls, worker2)
  @joinHTBuildParallel(&state.join_table1, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters2)

  // Pipeline 3
  @tlsReset(&tls, @sizeOf(ThreadState3), initTheadState3, teardownThreadState3, execCtx)
  @iterateTableParallel("customer", &state, &tls, worker3)
  @joinHTBuildParallel(&state.join_table2, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters3)

  // Pipeline 4
  @tlsReset(&tls, @sizeOf(ThreadState4), initTheadState4, teardownThreadState4, execCtx)
  @iterateTableParallel("orders", &state, &tls, worker4)
  @joinHTBuildParallel(&state.join_table3, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters4)

  // Pipeline 5
  @tlsReset(&tls, @sizeOf(ThreadState5), initTheadState5, teardownThreadState5, execCtx)
  @iterateTableParallel("lineitem", &state, &tls, worker5)
  @aggHTMoveParts(&state.agg_table2, &tls, off, mergerPartitions5)

  // Pipeline 6
  @tlsReset(&tls, @sizeOf(ThreadState6), initTheadState6, teardownThreadState6, execCtx)
  @aggHTParallelPartScan(&state.agg_table2, &state, &tls, worker6)
  @sorterSortParallel(&state.sorter, &tls, off)

  // Pipeline 7
  pipeline7(execCtx, &state)
  @outputFinalize(execCtx)

  return state.count
}
