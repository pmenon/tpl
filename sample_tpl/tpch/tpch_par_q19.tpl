struct OutputStruct {
  revenue : Real
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
  join_table : JoinHashTable
  revenue : RealSumAggregate
}

struct ThreadState1 {
  ts_join_table : JoinHashTable
}

struct ThreadState2 {
  ts_revenue : RealSumAggregate
}


struct JoinRow {
  p_partkey : Integer
  p_brand : StringVal
  p_container : StringVal
  p_size : Integer
}

fun checkJoinKey(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow) -> bool {
  if (@vpiGetInt(probe, 1) != build.p_partkey) {
    return false
  }
  return true
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow))
  @aggInit(&state.revenue)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table)
}

fun initTheadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow))
}

fun teardownThreadState1(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun initTheadState2(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
  @aggInit(&ts.ts_revenue)
}

fun teardownThreadState2(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
  @joinHTFree(&ts.ts_join_table)
}

fun gatherAgg(qs: *State, ts: *ThreadState2) -> nil {
  @aggMerge(&qs.revenue, &ts.ts_revenue)
}

// Scan part, build JHT
fun worker1(state: *State, ts: *ThreadState1, p_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(p_tvi)) {
    var vec = @tableIterGetVPI(p_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 0)) // p_partkey
      var build_row = @ptrCast(*JoinRow, @joinHTInsert(&ts.ts_join_table, hash_val))
      build_row.p_partkey = @vpiGetInt(vec, 0) // p_partkey
      build_row.p_brand = @vpiGetVarlen(vec, 3) // p_brand
      build_row.p_container = @vpiGetVarlen(vec, 6) // p_container
      build_row.p_size = @vpiGetInt(vec, 5) // p_size
    }
  }
}

// Scan lineitem, probe JHT, advance AGG
fun worker2(state: *State, ts: *ThreadState2, p_tvi: *TableVectorIterator) -> nil {
  // Used for predicates
  var brand12 = @stringToSql("Brand#12")
  var brand23 = @stringToSql("Brand#23")
  var brand34 = @stringToSql("Brand#34")
  var sm_container1 = @stringToSql("SM CASE")
  var sm_container2 = @stringToSql("SM BOX")
  var sm_container3 = @stringToSql("SM PACK")
  var sm_container4 = @stringToSql("SM PKG")
  var med_container1 = @stringToSql("MED BAG")
  var med_container2 = @stringToSql("MED BOX")
  var med_container3 = @stringToSql("MED PKG")
  var med_container4 = @stringToSql("MED PACK")
  var lg_container1 = @stringToSql("LG CASE")
  var lg_container2 = @stringToSql("LG BOX")
  var lg_container3 = @stringToSql("LG PACK")
  var lg_container4 = @stringToSql("LG PKG")
  var mode1 = @stringToSql("AIR")
  var mode2 = @stringToSql("AIR REG")
  var instruct = @stringToSql("DELIVER IN PERSON")
  for (@tableIterAdvance(p_tvi)) {
    var vec = @tableIterGetVPI(p_tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var hash_val = @hash(@vpiGetInt(vec, 1)) // l_partkey
      var hti: HashTableEntryIterator
      for (@joinHTLookup(&state.join_table, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey, state, vec);) {
        var join_row = @ptrCast(*JoinRow, @htEntryIterGetRow(&hti))
        if (
        // first pred
        ((join_row.p_brand == brand12)
        and (@vpiGetReal(vec, 4) >= 1.0 and @vpiGetReal(vec, 4) <= 11.0)
        and (join_row.p_size >= 1 and join_row.p_size <= 5)
        and (join_row.p_container == sm_container1 or join_row.p_container == sm_container2 or join_row.p_container == sm_container3 or join_row.p_container == sm_container4)
        and (@vpiGetVarlen(vec, 14) == mode1 or @vpiGetVarlen(vec, 14) == mode2)
        and (@vpiGetVarlen(vec, 13) == instruct)
        ) or
        ((join_row.p_brand == brand23)
        and (@vpiGetReal(vec, 4) >= 10.0 and @vpiGetReal(vec, 4) <= 20.0)
        and (join_row.p_size >= 1 and join_row.p_size <= 10)
        and (join_row.p_container == med_container1 or join_row.p_container == med_container2 or join_row.p_container == med_container3 or join_row.p_container == med_container4)
        and (@vpiGetVarlen(vec, 14) == mode1 or @vpiGetVarlen(vec, 14) == mode2)
        and (@vpiGetVarlen(vec, 13) == instruct)
        ) or
        ((join_row.p_brand == brand34)
        and (@vpiGetReal(vec, 4) >= 20.0 and @vpiGetReal(vec, 4) <= 30.0)
        and (join_row.p_size >= 1 and join_row.p_size <= 15)
        and (join_row.p_container == lg_container1 or join_row.p_container == lg_container2 or join_row.p_container == lg_container3 or join_row.p_container == lg_container4)
        and (@vpiGetVarlen(vec, 14) == mode1 or @vpiGetVarlen(vec, 14) == mode2)
        and (@vpiGetVarlen(vec, 13) == instruct)
        )
        ) {
          var input = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6))
          @aggAdvance(&ts.ts_revenue, &input)
        }
      }
    }
  }
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
  @joinHTBuildParallel(&state.join_table, &tls, off)
  //@tlsIterate(&tls, &state, gatherCounters1)

  // Pipeline 2
  @tlsReset(&tls, @sizeOf(ThreadState2), initTheadState2, teardownThreadState2, execCtx)
  @iterateTableParallel("lineitem", &state, &tls, worker2)
  @tlsIterate(&tls, &state, gatherAgg)

  // Output
  var out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
  out.revenue = @aggResult(&state.revenue)
  @outputFinalize(execCtx)

  return state.count
}
