struct OutputStruct {
  p_brand : StringVal
  p_type : StringVal
  p_size : Integer
  supplier_cnt : Integer
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
  agg_table : AggregationHashTable
  sorter : Sorter
}

struct ThreadState1 {
  ts_join_table : JoinHashTable
  filter: FilterManager
  ts_count: int32
}

struct ThreadState2 {
  ts_join_table : JoinHashTable
  filter: FilterManager
  ts_count: int32
}

struct ThreadState3 {
  ts_agg_table : AggregationHashTable
  ts_count: int32
}

struct ThreadState4 {
  ts_sorter : Sorter
  ts_count: int32
}

struct JoinRow1 {
  p_brand : StringVal
  p_type : StringVal
  p_size : Integer
  p_partkey : Integer
}

struct JoinRow2 {
  s_suppkey : Integer
}

struct AggValues {
  p_brand : StringVal
  p_type : StringVal
  p_size : Integer
  supplier_cnt : Integer
}

struct AggPayload {
  p_brand : StringVal
  p_type : StringVal
  p_size : Integer
  supplier_cnt : CountAggregate // TODO(Amadou): Replace by count disctinct aggregate
}

struct SorterRow {
  p_brand : StringVal
  p_type : StringVal
  p_size : Integer
  supplier_cnt : Integer
}


fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
  // ps_partkey == p_partkey
  if (@vpiGetInt(probe, 0) != build.p_partkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
  // ps_suppkey == s_suppkey
  if (@vpiGetInt(probe, 1) != build.s_suppkey) {
    return false
  }
  return true
}

fun checkAggKey(payload: *AggPayload, row: *AggValues) -> bool {
  if (payload.p_size != row.p_size) {
    return false
  }
  if (payload.p_brand != row.p_brand) {
    return false
  }
  if (payload.p_type != row.p_type) {
    return false
  }
  return true
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
  return ((agg_payload1.p_size == agg_payload2.p_size)
            and (agg_payload1.p_brand == agg_payload2.p_brand)
            and (agg_payload1.p_type == agg_payload2.p_type))
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.supplier_cnt < rhs.supplier_cnt) {
    return 1 // desc
  }
  if (lhs.supplier_cnt > rhs.supplier_cnt) {
    return -1 // desc
  }
  if (lhs.p_brand < rhs.p_brand) {
    return -1
  }
  if (lhs.p_brand > rhs.p_brand) {
    return 1
  }
  if (lhs.p_type < rhs.p_type) {
    return -1
  }
  if (lhs.p_type > rhs.p_type) {
    return 1
  }
  if (lhs.p_size < rhs.p_size) {
    return -1
  }
  if (lhs.p_size > rhs.p_size) {
    return 1
  }
  return 0
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
  var brand = @stringToSql("Brand#45")
  var pattern = @stringToSql("MEDIUM POLISHED%")

  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    @vpiMatch(vec, (@vpiGetInt(vec, 5) != 49) // p_size
                and (@vpiGetInt(vec, 5) != 14)
                and (@vpiGetInt(vec, 5) != 23)
                and (@vpiGetInt(vec, 5) != 45)
                and (@vpiGetInt(vec, 5) != 19)
                and (@vpiGetInt(vec, 5) != 3)
                and (@vpiGetInt(vec, 5) != 36)
                and (@vpiGetInt(vec, 5) != 9)
                and (@vpiGetVarlen(vec, 3) != brand)
                and !(@sqlToBool(@stringLike(@vpiGetVarlen(vec, 4), pattern))))
  }
  @vpiResetFiltered(vec)
  return 0
}

fun p2Filter1(vec: *VectorProjectionIterator) -> int32 {
  var pattern = @stringToSql("%instructions%requests%")
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    @vpiMatch(vec, @stringLike(@vpiGetVarlen(vec, 6), pattern))
  }
  @vpiResetFiltered(vec)
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @aggHTFree(&state.agg_table)
  @sorterFree(&state.sorter)
}

fun initTheadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p1Filter1)
  @filterManagerFinalize(&ts.filter)
}

fun teardownThreadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p2Filter1)
  @filterManagerFinalize(&ts.filter)
}

fun teardownThreadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  ts.ts_count = 0
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
}

fun teardownThreadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  @aggHTFree(&ts.ts_agg_table)
}

fun initTheadState4(execCtx: *ExecutionContext, ts: *ThreadState4) -> nil {
  ts.ts_count = 0
  @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownThreadState4(execCtx: *ExecutionContext, ts: *ThreadState4) -> nil {
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

// Scan part, build JHT1
fun worker1(state: *State, ts: *ThreadState1, p_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(p_tvi)) {
    var vec = @tableIterGetVPI(p_tvi)
    @filtersRun(&ts.filter, vec)
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // p_partkey
      var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row1.p_partkey = @vpiGetInt(vec, 0) // p_partkey
      build_row1.p_brand = @vpiGetVarlen(vec, 3) // p_brand
      build_row1.p_size = @vpiGetInt(vec, 5) // p_size
      build_row1.p_type = @vpiGetVarlen(vec, 4) // p_type
      //ts.ts_count = ts.ts_count + 1
    }
  }
}

// Scan supplier, build JHT2
fun worker2(state: *State, ts: *ThreadState2, s_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(s_tvi)) {
    var vec = @tableIterGetVPI(s_tvi)
    @filtersRun(&ts.filter, vec)
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // s_suppkey
      var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row2.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
      //ts.ts_count = ts.ts_count + 1
    }
  }
}

// Scan partsupp, probe JHT1, probe JHT2, build AHT
fun worker3(state: *State, ts: *ThreadState3, ps_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(ps_tvi)) {
    var vec = @tableIterGetVPI(ps_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // ps_partkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, state, vec);) {
        var join_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))
        // Anti-Join
        var hash_val2 = @hash(@vpiGetInt(vec, 1)) // ps_suppkey
        var hti2: HashTableEntryIterator
        @joinHTLookup(&state.join_table2, &hti2, hash_val2)
        if (!(@htEntryIterHasNext(&hti2, checkJoinKey2, state, vec))) {
          ts.ts_count = ts.ts_count + 1
          var agg_input : AggValues // Materialize
          agg_input.p_brand = join_row1.p_brand
          agg_input.p_type = join_row1.p_type
          agg_input.p_size = join_row1.p_size
          agg_input.supplier_cnt = @vpiGetInt(vec, 1) // ps_suppkey
          var agg_hash_val = @hash(agg_input.p_brand, agg_input.p_type, agg_input.p_size)
          var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey, &agg_input))
          if (agg_payload == nil) {
            agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
            agg_payload.p_brand = agg_input.p_brand
            agg_payload.p_type = agg_input.p_type
            agg_payload.p_size = agg_input.p_size
            @aggInit(&agg_payload.supplier_cnt)
          }
          @aggAdvance(&agg_payload.supplier_cnt, &agg_input.supplier_cnt)
        }
      }
    }
  }
}

fun mergerPartitions3(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload, @aggHTInsert(agg_table, partial_hash))
      agg_payload.p_brand = partial.p_brand
      agg_payload.p_type = partial.p_type
      agg_payload.p_size = partial.p_size
      @aggInit(&agg_payload.supplier_cnt)
    }
    @aggMerge(&agg_payload.supplier_cnt, &partial.supplier_cnt)
  }
}

// Scan Agg HT table, sort
fun worker4(state: *State, ts: *ThreadState4, agg_table: *AggregationHashTable) -> nil {
  var aht_iter: AHTIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&aht_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
    sorter_row.p_brand = agg_payload.p_brand
    sorter_row.p_type = agg_payload.p_type
    sorter_row.p_size = agg_payload.p_size
    sorter_row.supplier_cnt = @aggResult(&agg_payload.supplier_cnt)
    //ts.ts_count = ts.ts_count + 1
  }
  @aggHTIterClose(&aht_iter)
}

// Iterate through sorter, output
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    var out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    out.p_brand = sorter_row.p_brand
    out.p_type = sorter_row.p_type
    out.p_size = sorter_row.p_size
    out.supplier_cnt = sorter_row.supplier_cnt
    //state.count = state.count + 1
  }
  @sorterIterClose(&sort_iter)
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
  @iterateTableParallel("part", &state, &tls, worker1)
  @joinHTBuildParallel(&state.join_table1, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters1)

  // Pipeline 2
  @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
  @iterateTableParallel("supplier", &state, &tls, worker2)
  @joinHTBuildParallel(&state.join_table2, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters2)

  // Pipeline 3
  @tlsReset(&tls, @sizeOf(ThreadState3), initTheadState3, teardownThreadState3, execCtx)
  @iterateTableParallel("partsupp", &state, &tls, worker3)
  @aggHTMoveParts(&state.agg_table, &tls, off, mergerPartitions3)
  //@tlsIterate(&tls, &state, gatherCounters3)

  // Pipeline 4
  @tlsReset(&tls, @sizeOf(ThreadState4), initTheadState4, teardownThreadState4, execCtx)
  @aggHTParallelPartScan(&state.agg_table, &state, &tls, worker4)
  @sorterSortParallel(&state.sorter, &tls, off)

  // Pipeline 5
  pipeline5(execCtx, &state)
  @outputFinalize(execCtx)

  return state.count
}
