struct OutputStruct {
  ps_partkey : Integer
  value : Real
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
  n_nationkey : Integer
}

struct JoinRow2 {
  s_suppkey : Integer
}

struct AggPayload1 {
  value : RealSumAggregate
}

struct AggValues1 {
  value : Real
}

struct AggPayload2 {
  ps_partkey : Integer
  value : RealSumAggregate
}

struct AggValues2 {
  ps_partkey : Integer
  value : Real
}

struct SorterRow {
  ps_partkey : Integer
  value : Real
}

struct State {
  count: int32 // Debug
  join_table1 : JoinHashTable
  join_table2 : JoinHashTable
  agg1 : RealSumAggregate
  agg_table2 : AggregationHashTable
  sorter : Sorter
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow1) -> bool {
  // check s_nationkey == n_nationkey
  if (@pciGetInt(probe, 3) != build.n_nationkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow2) -> bool {
  // check ps_suppkey == s_suppkey
  if (@pciGetInt(probe, 1) != build.s_suppkey) {
    return false
  }
  return true
}

// Check that the aggregate key already exists
fun checkAggKey2(payload: *AggPayload2, row: *AggValues2) -> bool {
  if (payload.ps_partkey != row.ps_partkey) {
    return false
  }
  return true
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.value < rhs.value) {
    return -1
  }
  if (lhs.value > rhs.value) {
    return 1
  }
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @aggInit(&state.agg1)
  @aggHTInit(&state.agg_table2, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @aggHTFree(&state.agg_table2)
  @sorterFree(&state.sorter)
}


// Scan nation build JHT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  var n_tvi : TableVectorIterator
  @tableIterInit(&n_tvi, "nation")
  var germany = @stringToSql("GERMANY")
  for (@tableIterAdvance(&n_tvi)) {
    var vec = @tableIterGetPCI(&n_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetVarlen(vec, 1) == germany) {
        // Step 2: Insert into Hash Table
        var hash_val = @hash(@pciGetInt(vec, 0)) // n_nationkey
        var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
        build_row1.n_nationkey = @pciGetInt(vec, 0) // n_nationkey
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table1)
  @tableIterClose(&n_tvi)
}

// Scan supplier, scan JHT1, build JHT2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  var s_tvi : TableVectorIterator
  @tableIterInit(&s_tvi, "supplier")
  for (@tableIterAdvance(&s_tvi)) {
    var vec = @tableIterGetPCI(&s_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Probe JHT1
      // Step 2: Probe HT1
      var hash_val = @hash(@pciGetInt(vec, 3)) // s_nationkey
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&state.join_table1, &hti, hash_val); @joinHTIterHasNext(&hti, checkJoinKey1, execCtx, vec);) {
        var join_row1 = @ptrCast(*JoinRow1, @joinHTIterGetRow(&hti))

        // Step 3: Build HT2
        var hash_val2 = @hash(@pciGetInt(vec, 0)) // s_suppkey
        var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val2))
        build_row2.s_suppkey = @pciGetInt(vec, 0)
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table2)
  @tableIterClose(&s_tvi)
}

// Scan partsupp, probe HT2, advance agg1
fun pipeline3_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var ps_tvi : TableVectorIterator
  @tableIterInit(&ps_tvi, "partsupp")
  for (@tableIterAdvance(&ps_tvi)) {
    var vec = @tableIterGetPCI(&ps_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var hash_val = @hash(@pciGetInt(vec, 1)) // ps_suppkey
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&state.join_table2, &hti, hash_val); @joinHTIterHasNext(&hti, checkJoinKey2, execCtx, vec);) {
        var join_row2 = @ptrCast(*JoinRow2, @joinHTIterGetRow(&hti))
        var agg_input = @pciGetDouble(vec, 3) * @pciGetInt(vec, 2)
        @aggAdvance(&state.agg1, &agg_input)
      }
    }
  }
}

// Scan partsupp, probe HT2, build agg
fun pipeline3_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var ps_tvi : TableVectorIterator
  @tableIterInit(&ps_tvi, "partsupp")
  for (@tableIterAdvance(&ps_tvi)) {
    var vec = @tableIterGetPCI(&ps_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var hash_val = @hash(@pciGetInt(vec, 1)) // ps_suppkey
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&state.join_table2, &hti, hash_val); @joinHTIterHasNext(&hti, checkJoinKey2, execCtx, vec);) {
        var join_row2 = @ptrCast(*JoinRow2, @joinHTIterGetRow(&hti))
        var agg_input : AggValues2 // Materialize
        agg_input.ps_partkey = @pciGetInt(vec, 0)
        agg_input.value = @pciGetDouble(vec, 3) * @pciGetInt(vec, 2)
        var agg_hash_val = @hash(agg_input.ps_partkey)
        var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(&state.agg_table2, agg_hash_val, checkAggKey2, &agg_input))
        if (agg_payload == nil) {
          agg_payload = @ptrCast(*AggPayload2, @aggHTInsert(&state.agg_table2, agg_hash_val))
          agg_payload.ps_partkey = agg_input.ps_partkey
          @aggInit(&agg_payload.value)
        }
        @aggAdvance(&agg_payload.value, &agg_input.value)
      }
    }
  }
}

// BNL, sort
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var agg_iter = &agg_ht_iter
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(agg_iter, &state.agg_table2); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
    var agg_payload = @ptrCast(*AggPayload2, @aggHTIterGetRow(agg_iter))
    if (@aggResult(&agg_payload.value) > (@aggResult(&state.agg1) * 0.0001)) {
      // Step 2: Build Sorter
      var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
      sorter_row.ps_partkey = agg_payload.ps_partkey
      sorter_row.value = @aggResult(&agg_payload.value)
    }
  }
  @sorterSort(&state.sorter)
  @aggHTIterClose(agg_iter)
}

// Iterate through sorter, output
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    var out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.ps_partkey = sorter_row.ps_partkey
    out.value = sorter_row.value
    state.count = state.count + 1
  }
  @sorterIterClose(&sort_iter)
}


fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  pipeline1(execCtx, state)
  pipeline2(execCtx, state)
  pipeline3_1(execCtx, state)
  pipeline3_2(execCtx, state)
  pipeline4(execCtx, state)
  pipeline5(execCtx, state)
  @outputFinalize(execCtx)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}
