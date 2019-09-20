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
  match_flag: bool // for semi-join
}

// Input & Output of the aggregator
struct AggRow {
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

// Check that two join keys are equal
fun checkJoinKey(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build_row: *JoinBuildRow) -> bool {
  // l_orderkey == o_orderkey
  return @sqlToBool(@pciGetInt(probe, 0) == build_row.o_orderkey)
}

// Check that the aggregate key already exists
fun checkAggKey(agg: *AggRow, build_row: *JoinBuildRow) -> bool {
  return @sqlToBool(agg.o_orderpriority == build_row.o_orderpriority)
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


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggRow))
  @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
    @joinHTFree(&state.join_table)
}


// Pipeline 1 (Join Build)
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  var o_tvi : TableVectorIterator
  var oids: [3]uint32
  oids[0] = 6 // o_orderpriority : varchar
  oids[1] = 1 // o_orderkey : int
  oids[2] = 5 // o_orderdate : date

  @tableIterInitBind(&o_tvi, execCtx, "orders", oids)
  for (@tableIterAdvance(&o_tvi)) {
    var vec = @tableIterGetPCI(&o_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetDate(vec, 2) >= @dateToSql(1993, 07, 01) // o_orderdate
          and @pciGetDate(vec, 2) <= @dateToSql(1993, 10, 01)) { // o_orderdate
        // Step 2: Insert into Hash Table
        var hash_val = @hash(@pciGetInt(vec, 1)) // o_orderkey
        var build_row = @ptrCast(*JoinBuildRow, @joinHTInsert(&state.join_table, hash_val))
        build_row.o_orderkey = @pciGetInt(vec, 1) // o_orderkey
        build_row.o_orderpriority = @pciGetVarlen(vec, 0) // o_orderpriority
        build_row.match_flag = false
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table)
  // Close Iterator
  @tableIterClose(&o_tvi)
}

// Pipeline 2 (Join Probe up to Agg)
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  var l_tvi : TableVectorIterator
  var oids: [3]uint32
  oids[0] = 1 // l_orderkey : int
  oids[1] = 12 // l_commitdate : date
  oids[2] = 13 // l_receiptdate : date
  @tableIterInitBind(&l_tvi, execCtx, "lineitem", oids)
  for (@tableIterAdvance(&l_tvi)) {
    var vec = @tableIterGetPCI(&l_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // if l_commitdate < l_receiptdate
      if (@pciGetDate(vec, 1) < @pciGetDate(vec, 2)) {
        // Step 2: Probe Join Hash Table
        var hash_val = @hash(@pciGetInt(vec, 0)) // l_orderkey
        var join_iter: JoinHashTableIterator
        for (@joinHTIterInit(&join_iter, &state.join_table, hash_val); @joinHTIterHasNext(&join_iter, checkJoinKey, execCtx, vec);) {
          var build_row = @ptrCast(*JoinBuildRow, @joinHTIterGetRow(&join_iter))
          // match each row once
          if (!build_row.match_flag) {
            build_row.match_flag = true
            // Step 3: Build Agg Hash Table
            var agg_hash_val = @hash(build_row.o_orderpriority)
            var agg = @ptrCast(*AggRow, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, build_row))
            if (agg == nil) {
              agg = @ptrCast(*AggRow, @aggHTInsert(&state.agg_table, agg_hash_val))
              agg.o_orderpriority = build_row.o_orderpriority
              @aggInit(&agg.order_count)
            }
            @aggAdvance(&agg.order_count, &build_row.o_orderpriority)
          }
        }
      }
    }
  }
  // Close Iterator
  @tableIterClose(&l_tvi)
}

// Pipeline 3 (Sort)
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
  var aht_iter: AggregationHashTableIterator
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(&aht_iter, &state.agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
    var agg = @ptrCast(*AggRow, @aggHTIterGetRow(&aht_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
    sorter_row.o_orderpriority = agg.o_orderpriority
    sorter_row.order_count = @aggResult(&agg.order_count)
  }
  @sorterSort(&state.sorter)
  @aggHTIterClose(&aht_iter)
}

// Pipeline 4 (Output)
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  var out: *OutputStruct
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.o_orderpriority = sorter_row.o_orderpriority
    out.order_count = sorter_row.order_count
  }
  @sorterIterClose(&sort_iter)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  pipeline1(execCtx, state)
  pipeline2(execCtx, state)
  pipeline3(execCtx, state)
  pipeline4(execCtx, state)
  @outputFinalize(execCtx)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}
