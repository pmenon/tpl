struct Output {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : Real
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

struct JoinRow1 {
  n1_nationkey : Integer
  n2_nationkey : Integer
  n1_name : StringVal
  n2_name : StringVal
}

struct JoinRow2 {
  n1_nationkey : Integer
  n1_name : StringVal
  n2_name : StringVal
  c_custkey : Integer
}

struct JoinRow3 {
  n1_nationkey : Integer
  n1_name : StringVal
  n2_name : StringVal
  o_orderkey : Integer
}

struct JoinRow4 {
  s_suppkey : Integer
  s_nationkey : Integer
}

struct JoinProbe4 {
  n1_nationkey : Integer
  l_suppkey : Integer
}

struct AggPayload {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : RealSumAggregate
}

struct AggValues {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : Real
}

struct SorterRow {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : Real
}

struct State {
  count: int32 // Debug
  join_table1: JoinHashTable
  join_table2: JoinHashTable
  join_table3: JoinHashTable
  join_table4: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
}

struct ThreadState1 {
  ts_join_table : JoinHashTable
  filter: FilterManager
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
  filter: FilterManager
  ts_count: int32
}

struct ThreadState6 {
  ts_sorter: Sorter
  ts_count: int32
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
  var france = @stringToSql("FRANCE")
  var germany = @stringToSql("GERMANY")
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    // n_name = "france" or n_name = "germany"
    @vpiMatch(vec, @vpiGetVarlen(vec, 1) == france or @vpiGetVarlen(vec, 1) == germany)
  }
  @vpiResetFiltered(vec)
  return 0
}

fun p5Filter1(vec: *VectorProjectionIterator) -> int32 {
  var lo = @dateToSql(1995, 1, 1)
  var hi = @dateToSql(1996, 12, 31)
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    // l_shipdate between lo and hi
    @vpiMatch(vec, @vpiGetDate(vec, 10) >= lo and @vpiGetDate(vec, 10) <= hi)
  }
  @vpiResetFiltered(vec)
  return 0
}

// Check that the aggregate key already exists
fun checkAggKey(payload: *AggPayload, row: *AggValues) -> bool {
  if (payload.l_year != row.l_year) {
    return false
  }
  if (payload.supp_nation != row.supp_nation) {
    return false
  }
  if (payload.cust_nation != row.cust_nation) {
    return false
  }
  return true
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
  return ((agg_payload1.l_year == agg_payload2.l_year)
            and (agg_payload1.supp_nation == agg_payload2.supp_nation)
            and (agg_payload1.cust_nation == agg_payload2.cust_nation))
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.supp_nation < rhs.supp_nation) {
    return -1
  }
  if (lhs.supp_nation > rhs.supp_nation) {
    return 1
  }
  if (lhs.cust_nation < rhs.cust_nation) {
    return -1
  }
  if (lhs.cust_nation > rhs.cust_nation) {
    return 1
  }
  if (lhs.l_year < rhs.l_year) {
    return -1
  }
  if (lhs.l_year > rhs.l_year) {
    return 1
  }
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
  @joinHTInit(&state.join_table4, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @joinHTFree(&state.join_table3)
  @joinHTFree(&state.join_table4)
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
}

fun teardownThreadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
}

fun teardownThreadState3(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState4(execCtx: *ExecutionContext, ts: *ThreadState4) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
}

fun teardownThreadState4(execCtx: *ExecutionContext, ts: *ThreadState4) -> nil {
  @joinHTFree(&ts.ts_join_table)
}


fun initTheadState5(execCtx: *ExecutionContext, ts: *ThreadState5) -> nil {
  ts.ts_count = 0
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p5Filter1)
  @filterManagerFinalize(&ts.filter)
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

fun gatherCounters5(qs: *State, ts: *ThreadState5) -> nil {
  qs.count = qs.count + ts.ts_count
}

fun gatherCounters6(qs: *State, ts: *ThreadState6) -> nil {
  qs.count = qs.count + ts.ts_count
}


fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
  // check c_nationkey == n2_nationkey
  if (@vpiGetInt(probe, 3) != build.n2_nationkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
  // o_custkey == c_custkey
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

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *JoinProbe4, build: *JoinRow4) -> bool {
  // l_suppkey == s_suppkey
  if (probe.l_suppkey != build.s_suppkey) {
    return false
  }
  // n1_nationkey == s_nationkey
  if (probe.n1_nationkey != build.s_nationkey) {
    return false
  }
  return true
}

// BNL nation with nation, then build JHT1
fun worker1(state: *State, ts: *ThreadState1, n1_tvi: *TableVectorIterator) -> nil {
  var n2_tvi : TableVectorIterator
  var france = @stringToSql("FRANCE")
  var germany = @stringToSql("GERMANY")
  for (@tableIterAdvance(n1_tvi)) {
    var vec1 = @tableIterGetVPI(n1_tvi)
    @filtersRun(&ts.filter, vec1)
    @tableIterInit(&n2_tvi, "nation")
    for (; @vpiHasNextFiltered(vec1); @vpiAdvanceFiltered(vec1)) {
      @tableIterInit(&n2_tvi, "nation")
      for (@tableIterAdvance(&n2_tvi)) {
        var vec2 = @tableIterGetVPI(&n2_tvi)
        for (; @vpiHasNext(vec2); @vpiAdvance(vec2)) {
          if ((@vpiGetVarlen(vec1, 1) == france and @vpiGetVarlen(vec2, 1) == germany) or @vpiGetVarlen(vec1, 1) == germany and @vpiGetVarlen(vec2, 1) == france) {
            // Build JHT1
            var hash_val = @hash(@vpiGetInt(vec2, 0)) // n2_nationkey
            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row1.n1_nationkey = @vpiGetInt(vec1, 0) // n1_nationkey
            build_row1.n2_nationkey = @vpiGetInt(vec2, 0) // n2_nationkey
            build_row1.n1_name = @vpiGetVarlen(vec1, 1) // n1_name
            build_row1.n2_name = @vpiGetVarlen(vec2, 1) // n2_name
            ts.ts_count = ts.ts_count + 1
          }
        }
      }
      @tableIterClose(&n2_tvi)
    }
  }
}

// Scan Customer, probe JHT1, build JHT2
fun worker2(state: *State, ts: *ThreadState2, c_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(c_tvi)) {
    var vec = @tableIterGetVPI(c_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      // Step 2: Probe JHT1
      var hash_val = @hash(@vpiGetInt(vec, 3)) // c_nationkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, state, vec);) {
        var join_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))

        // Step 3: Insert into JHT2
        var hash_val2 = @hash(@vpiGetInt(vec, 0)) // c_custkey
        var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val2))
        build_row2.n1_nationkey = join_row1.n1_nationkey
        build_row2.n1_name = join_row1.n1_name
        build_row2.n2_name = join_row1.n2_name
        build_row2.c_custkey = @vpiGetInt(vec, 0) // c_custkey
      }
    }
  }
}

// Scan orders, probe JHT2, build JHT3
fun worker3(state: *State, ts: *ThreadState3, o_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(o_tvi)) {
    var vec = @tableIterGetVPI(o_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      // Step 2: Probe JHT2
      var hash_val = @hash(@vpiGetInt(vec, 1)) // o_custkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, state, vec);) {
        var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))

        // Step 3: Insert into join table 3
        var hash_val3 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
        var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&ts.ts_join_table, hash_val3))
        build_row3.n1_nationkey = join_row2.n1_nationkey
        build_row3.n1_name = join_row2.n1_name
        build_row3.n2_name = join_row2.n2_name
        build_row3.o_orderkey = @vpiGetInt(vec, 0)
      }
    }
  }
}

// Scan supplier, build JHT4
fun worker4(state: *State, ts: *ThreadState4, s_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(s_tvi)) {
    var vec = @tableIterGetVPI(s_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
        var hash_val = @hash(@vpiGetInt(vec, 0), @vpiGetInt(vec, 3)) // s_suppkey, s_nationkey
        var build_row4 = @ptrCast(*JoinRow4, @joinHTInsert(&ts.ts_join_table, hash_val))
        build_row4.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
        build_row4.s_nationkey = @vpiGetInt(vec, 3) // s_nationkey
      }
    }
  }
}

// Scan lineitem, probe JHT3, probe JHT4, build AHT
fun worker5(state: *State, ts: *ThreadState5, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    @filtersRun(&ts.filter, vec)
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      // Step 2: Probe JHT3
      var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
      var hti3: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table3, &hti3, hash_val); @htEntryIterHasNext(&hti3, checkJoinKey3, state, vec);) {
        var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti3))

        // Step 3: Probe JHT4
        var hash_val4 = @hash(@vpiGetInt(vec, 2), join_row3.n1_nationkey) // l_suppkey
        var join_probe4 : JoinProbe4 // Materialize the right pipeline
        join_probe4.l_suppkey = @vpiGetInt(vec, 2)
        join_probe4.n1_nationkey = join_row3.n1_nationkey
        var hti4: HashTableEntryIterator
        for (@joinHTLookup(&state.join_table4, &hti4, hash_val4); @htEntryIterHasNext(&hti4, checkJoinKey4, state, &join_probe4);) {
          var join_row4 = @ptrCast(*JoinRow4, @htEntryIterGetRow(&hti4))

          // Step 4: Build Agg HT
          var agg_input : AggValues // Materialize
          agg_input.supp_nation = join_row3.n1_name
          agg_input.cust_nation = join_row3.n2_name
          agg_input.l_year = @extractYear(@vpiGetDate(vec, 10))
          agg_input.volume = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6)) // l_extendedprice * (1.0 -  l_discount)
          var agg_hash_val = @hash(agg_input.supp_nation, agg_input.cust_nation, agg_input.l_year)
          var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey, &agg_input))
          if (agg_payload == nil) {
            agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
            agg_payload.supp_nation = agg_input.supp_nation
            agg_payload.cust_nation = agg_input.cust_nation
            agg_payload.l_year = agg_input.l_year
            @aggInit(&agg_payload.volume)
          }
          @aggAdvance(&agg_payload.volume, &agg_input.volume)
        }
      }
    }
  }
}

fun mergerPartitions5(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload, @aggHTInsert(agg_table, partial_hash))
      agg_payload.supp_nation = partial.supp_nation
      agg_payload.cust_nation = partial.cust_nation
      agg_payload.l_year = partial.l_year
      @aggInit(&agg_payload.volume)
    }
    @aggMerge(&agg_payload.volume, &partial.volume)
  }
}

// Scan AHT, sort
fun worker6(state: *State, ts: *ThreadState6, agg_table: *AggregationHashTable) -> nil {
  var x = 0
  var aht_iter: AHTIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&aht_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
    sorter_row.supp_nation = agg_payload.supp_nation
    sorter_row.cust_nation = agg_payload.cust_nation
    sorter_row.l_year = agg_payload.l_year
    sorter_row.volume = @aggResult(&agg_payload.volume)
    ts.ts_count = ts.ts_count + 1
  }
  @aggHTIterClose(&aht_iter)
}

fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
  var x = 0
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    var out = @ptrCast(*Output, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.supp_nation = sorter_row.supp_nation
    out.cust_nation = sorter_row.cust_nation
    out.l_year = sorter_row.l_year
    out.volume = sorter_row.volume
    state.count = state.count + 1
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
  @iterateTableParallel("nation", &state, &tls, worker1)
  @joinHTBuildParallel(&state.join_table1, &tls, off)
  @tlsIterate(&tls, &state, gatherCounters1)

  // Pipeline 2
  @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
  @iterateTableParallel("customer", &state, &tls, worker2)
  @joinHTBuildParallel(&state.join_table2, &tls, off)
  @tlsIterate(&tls, &state, gatherCounters2)

  // Pipeline 3
  @tlsReset(&tls, @sizeOf(ThreadState3), initTheadState3, teardownThreadState3, execCtx)
  @iterateTableParallel("orders", &state, &tls, worker3)
  @joinHTBuildParallel(&state.join_table3, &tls, off)

  // Pipeline 4
  @tlsReset(&tls, @sizeOf(ThreadState4), initTheadState4, teardownThreadState4, execCtx)
  @iterateTableParallel("supplier", &state, &tls, worker4)
  @joinHTBuildParallel(&state.join_table4, &tls, off)

  // Pipeline 5
  @tlsReset(&tls, @sizeOf(ThreadState5), initTheadState5, teardownThreadState5, execCtx)
  @iterateTableParallel("lineitem", &state, &tls, worker5)
  @aggHTMoveParts(&state.agg_table, &tls, off, mergerPartitions5)

  // Pipeline 6
  @tlsReset(&tls, @sizeOf(ThreadState6), initTheadState6, teardownThreadState6, execCtx)
  @aggHTParallelPartScan(&state.agg_table, &state, &tls, worker6)
  @sorterSortParallel(&state.sorter, &tls, off)

  // Pipeline 7
  pipeline7(execCtx, &state)
  @outputFinalize(execCtx)

  return state.count
}
