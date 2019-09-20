// The code is written to (almost) match the way it will be codegened, so it may be overly verbose.
// But technically, some of these structs are redundant (like AggValues or SorterRow)

struct OutputStruct {
  n_name : StringVal
  revenue: Real
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
  join_table1: JoinHashTable
  join_table2: JoinHashTable
  join_table3: JoinHashTable
  join_table4: JoinHashTable
  join_table5: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
  count: int32 // For debugging
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
  filter: FilterManager
  ts_count: int32
}

struct ThreadState5 {
  ts_join_table : JoinHashTable
  ts_count: int32
}

struct ThreadState6 {
  ts_agg_table: AggregationHashTable
  ts_count: int32
}

struct ThreadState7 {
  ts_sorter: Sorter
  ts_count: int32
}


struct JoinRow1 {
  r_regionkey : Integer
}

struct JoinRow2 {
  n_name : StringVal
  n_nationkey : Integer
}

struct JoinRow3 {
  n_name : StringVal
  n_nationkey : Integer
  c_custkey : Integer
}

struct JoinRow4 {
  n_name : StringVal
  n_nationkey : Integer
  o_orderkey : Integer
}

struct JoinProbe5 {
  n_nationkey : Integer
  l_suppkey : Integer
}

struct JoinRow5 {
  s_suppkey : Integer
  s_nationkey : Integer
}


// Aggregate payload
struct AggPayload {
  n_name: StringVal
  revenue : RealSumAggregate
}

// Input of aggregate
struct AggValues {
  n_name: StringVal
  revenue : Real
}

// Input and Output of sorter
struct SorterRow {
  n_name: StringVal
  revenue : Real
}

fun checkAggKeyFn(payload: *AggPayload, row: *AggValues) -> bool {
  if (payload.n_name != row.n_name) {
    return false
  }
  return true
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
  return @sqlToBool(agg_payload1.n_name == agg_payload2.n_name)
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
  // check n_regionkey == r_regionkey
  if (@vpiGetInt(probe, 2) != build.r_regionkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
  // check c_nationkey == n_nationkey
  if (@vpiGetInt(probe, 3) != build.n_nationkey) {
    return false
  }
  return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow3) -> bool {
  // o_custkey == c_custkey
  if (@vpiGetInt(probe, 1) != build.c_custkey) {
    return false
  }
  return true
}

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow4) -> bool {
  // l_orderkey == o_orderkey
  if (@vpiGetInt(probe, 0) != build.o_orderkey) {
    return false
  }
  return true
}

fun checkJoinKey5(execCtx: *ExecutionContext, probe: *JoinProbe5, build: *JoinRow5) -> bool {
  if (probe.n_nationkey != build.s_nationkey) {
    return false
  }
  // l_suppkey == s_suppkey
  if (probe.l_suppkey != build.s_suppkey) {
    return false
  }
  return true
}

fun checkAggKey(payload: *AggPayload, values: *AggValues) -> bool {
  if (payload.n_name != values.n_name) {
    return false
  }
  return true
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.revenue < rhs.revenue) {
    return -1
  }
  if (lhs.revenue > rhs.revenue) {
    return 1
  }
  return 0
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
  var param = @stringToSql("ASIA")
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    // r_name = "ASIA"
    @vpiMatch(vec, @vpiGetVarlen(vec, 1) == param)
  }
  @vpiResetFiltered(vec)
  return 0
}

fun p4Filter1(vec: *VectorProjectionIterator) -> int32 {
  var lo = @dateToSql(1990, 1, 1)
  var hi = @dateToSql(2000, 1, 1)
  for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
    // o_orderdate between lo and hi
    @vpiMatch(vec, @vpiGetDate(vec, 4) >= lo and @vpiGetDate(vec, 4) <= hi)
  }
  @vpiResetFiltered(vec)
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  // Initialize hash tables
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
  @joinHTInit(&state.join_table4, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
  @joinHTInit(&state.join_table5, @execCtxGetMem(execCtx), @sizeOf(JoinRow5))

  // Initialize aggregate
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))

  // Initialize Sorter
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @joinHTFree(&state.join_table3)
  @joinHTFree(&state.join_table4)
  @joinHTFree(&state.join_table5)
  @aggHTFree(&state.agg_table)
  @sorterFree(&state.sorter)
  @outputFinalize(execCtx)
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
  @filterManagerInit(&ts.filter)
  @filterManagerInsertFilter(&ts.filter, p4Filter1)
  @filterManagerFinalize(&ts.filter)
}

fun teardownThreadState4(execCtx: *ExecutionContext, ts: *ThreadState4) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState5(execCtx: *ExecutionContext, ts: *ThreadState5) -> nil {
  ts.ts_count = 0
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow5))
}

fun teardownThreadState5(execCtx: *ExecutionContext, ts: *ThreadState5) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState6(execCtx: *ExecutionContext, ts: *ThreadState6) -> nil {
  ts.ts_count = 0
  @aggHTInit(&ts.ts_agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
}

fun teardownThreadState6(execCtx: *ExecutionContext, ts: *ThreadState6) -> nil {
  @aggHTFree(&ts.ts_agg_table)
}

fun initTheadState7(execCtx: *ExecutionContext, ts: *ThreadState7) -> nil {
  ts.ts_count = 0
  @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownThreadState7(execCtx: *ExecutionContext, ts: *ThreadState7) -> nil {
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

fun gatherCounters7(qs: *State, ts: *ThreadState7) -> nil {
  qs.count = qs.count + ts.ts_count
}

// Scan Region table and build HT1
fun worker1(state: *State, ts: *ThreadState1, r_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(r_tvi)) {
    var vec = @tableIterGetVPI(r_tvi)
    @filtersRun(&ts.filter, vec)
    // Step 2: Insert into HT1
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // r_regionkey
      var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row1.r_regionkey = @vpiGetInt(vec, 0) // r_regionkey
    }
  }
}

// Scan Nation table, probe HT1, build HT2
fun worker2(state: *State, ts: *ThreadState2, n_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(n_tvi)) {
    var vec = @tableIterGetVPI(n_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      // Step 2: Probe HT1
      var hash_val = @hash(@vpiGetInt(vec, 2)) // n_regionkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, state, vec);) {
        var build_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))

        // Step 3: Insert into join table 2
        var hash_val2 = @hash(@vpiGetInt(vec, 0)) // n_nationkey
        var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val2))
        build_row2.n_nationkey = @vpiGetInt(vec, 0) // n_nationkey
        build_row2.n_name = @vpiGetVarlen(vec, 1) // n_name
      }
    }
  }
}

// Scan Customer table, probe HT2, build HT3
fun worker3(state: *State, ts: *ThreadState3, c_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(c_tvi)) {
    var vec = @tableIterGetVPI(c_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      // Step 2: Probe HT2
      var hash_val = @hash(@vpiGetInt(vec, 3)) // c_nationkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, state, vec);) {
        var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))
        // Step 3: Insert into join table 3
        var hash_val3 = @hash(@vpiGetInt(vec, 0)) // c_custkey
        var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&ts.ts_join_table, hash_val3))
        build_row3.n_nationkey = join_row2.n_nationkey
        build_row3.n_name = join_row2.n_name
        build_row3.c_custkey = @vpiGetInt(vec, 0) // c_custkey
      }
    }
  }
}

// Scan Orders table, probe HT3, build HT4
fun worker4(state: *State, ts: *ThreadState4, o_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(o_tvi)) {
    var vec = @tableIterGetVPI(o_tvi)
    @filtersRun(&ts.filter, vec)
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      // Step 2: Probe HT3
      var hash_val = @hash(@vpiGetInt(vec, 1)) // o_custkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table3, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey3, state, vec);) {
        var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti))
        // Step 3: Insert into join table 4
        var hash_val4 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
        var build_row4 = @ptrCast(*JoinRow4, @joinHTInsert(&ts.ts_join_table, hash_val4))
        build_row4.n_nationkey = join_row3.n_nationkey
        build_row4.n_name = join_row3.n_name
        build_row4.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
        ts.ts_count = ts.ts_count + 1
      }
    }
  }
}

// Scan Supplier, build join HT5
fun worker5(state: *State, ts: *ThreadState5, s_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(s_tvi)) {
    var vec = @tableIterGetVPI(s_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      // Step 2: Insert into HT5
      var hash_val = @hash(@vpiGetInt(vec, 0), @vpiGetInt(vec, 3)) // s_suppkey, s_nationkey
      var build_row5 = @ptrCast(*JoinRow5, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row5.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
      build_row5.s_nationkey = @vpiGetInt(vec, 3) // s_nationkey
      ts.ts_count = ts.ts_count + 1
    }
  }
}


fun worker6(state: *State, ts: *ThreadState6, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      // Step 2: Probe HT4
      var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table4, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey4, state, vec);) {
        var join_row4 = @ptrCast(*JoinRow4, @htEntryIterGetRow(&hti))

        // Step 3: Probe HT5
        var hash_val5 = @hash(@vpiGetInt(vec, 2), join_row4.n_nationkey) // l_suppkey
        var join_probe5 : JoinProbe5 // Materialize the right pipeline
        join_probe5.n_nationkey = join_row4.n_nationkey
        join_probe5.l_suppkey = @vpiGetInt(vec, 2)
        var hti5: HashTableEntryIterator
        for (@joinHTLookup(&state.join_table5, &hti5, hash_val5); @htEntryIterHasNext(&hti5, checkJoinKey5, state, &join_probe5);) {
          var join_row5 = @ptrCast(*JoinRow5, @htEntryIterGetRow(&hti5))
          // Step 4: Build Agg HT
          var agg_input : AggValues // Materialize
          agg_input.n_name = join_row4.n_name
          agg_input.revenue = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6)) // l_extendedprice * (1.0 -  l_discount)
          var agg_hash_val = @hash(join_row4.n_name)
          var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey, &agg_input))
          if (agg_payload == nil) {
            agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
            agg_payload.n_name = agg_input.n_name
            @aggInit(&agg_payload.revenue)
          }
          @aggAdvance(&agg_payload.revenue, &agg_input.revenue)
        }
      }
    }
  }
}

fun mergerPartitions6(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
    if (agg_payload == nil) {
      agg_payload = @ptrCast(*AggPayload, @aggHTInsert(agg_table, partial_hash))
      agg_payload.n_name = partial.n_name
      @aggInit(&agg_payload.revenue)
    }
    @aggMerge(&agg_payload.revenue, &partial.revenue)
  }
}

// Scan Agg HT table, sort
fun worker7(state: *State, ts: *ThreadState7, agg_table: *AggregationHashTable) -> nil {
  var aht_iter: AHTIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&aht_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
    sorter_row.n_name = agg_payload.n_name
    sorter_row.revenue = @aggResult(&agg_payload.revenue)
    ts.ts_count = ts.ts_count + 1
  }
  @aggHTIterClose(&aht_iter)
}

// Scan sorter, output
fun pipeline8(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  var out: *OutputStruct
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.n_name = sorter_row.n_name
    out.revenue = sorter_row.revenue
  }
  @outputFinalize(execCtx)
  @sorterIterClose(&sort_iter)
}


fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  // set up state
  setUpState(execCtx, &state)
  var off: uint32 = 0

  // Pipeline 1
  var tls : ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState1), initTheadState1, teardownThreadState1, execCtx)
  @iterateTableParallel("region", &state, &tls, worker1)
  @joinHTBuildParallel(&state.join_table1, &tls, off)


  // Pipeline 2
  @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
  @iterateTableParallel("nation", &state, &tls, worker2)
  @joinHTBuildParallel(&state.join_table2, &tls, off)

  // Pipeline 3
  @tlsReset(&tls, @sizeOf(ThreadState3), initTheadState3, teardownThreadState3, execCtx)
  @iterateTableParallel("customer", &state, &tls, worker3)
  @joinHTBuildParallel(&state.join_table3, &tls, off)

  // Pipeline 4
  @tlsReset(&tls, @sizeOf(ThreadState4), initTheadState4, teardownThreadState4, execCtx)
  @iterateTableParallel("orders", &state, &tls, worker4)
  @joinHTBuildParallel(&state.join_table4, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters4)

  // Pipeline 5
  @tlsReset(&tls, @sizeOf(ThreadState5), initTheadState5, teardownThreadState5, execCtx)
  @iterateTableParallel("supplier", &state, &tls, worker5)
  @joinHTBuildParallel(&state.join_table5, &tls, off)

  // Pipeline 6
  @tlsReset(&tls, @sizeOf(ThreadState6), initTheadState6, teardownThreadState6, execCtx)
  @iterateTableParallel("lineitem", &state, &tls, worker6)
  @aggHTMoveParts(&state.agg_table, &tls, off, mergerPartitions6)

  // Pipeline 7
  @tlsReset(&tls, @sizeOf(ThreadState7), initTheadState7, teardownThreadState7, execCtx)
  @aggHTParallelPartScan(&state.agg_table, &state, &tls, worker7)
  @sorterSortParallel(&state.sorter, &tls, off)

  // Pipeline 8
  pipeline8(execCtx, &state)
  @outputFinalize(execCtx)

  return state.count
}
