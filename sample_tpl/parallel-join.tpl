struct State {
  jht: JoinHashTable
  num_matches: uint32
}

struct ThreadState_1 {
  jht: JoinHashTable
  filter: FilterManager
}

struct ThreadState_2 {
  num_matches: uint32
}

struct BuildRow {
  key: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  // Complex bits
  @joinHTInit(&state.jht, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  // Simple bits
  state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.jht)
}

fun _1_Lt500(vpi: *VectorProjectionIterator) -> int32 {
  var param: Integer = @intToSql(500)
  var cola: Integer
  if (@vpiIsFiltered(vpi)) {
    for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
      cola = @vpiGetInt(vpi, 0)
      @vpiMatch(vpi, cola < param)
    }
  } else {
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      cola = @vpiGetInt(vpi, 0)
      @vpiMatch(vpi, cola < param)
    }
  }
  @vpiResetFiltered(vpi)
  return 0
}

fun _1_Lt500_Vec(vpi: *VectorProjectionIterator) -> int32 {
  var filter: VectorFilterExecutor
  @filterExecInit(&filter, vpi)
  @filterExecLt(&filter, 0, @intToSql(500))
  @filterExecFinish(&filter)
  @filterExecFree(&filter)
  return 0
}

fun _1_pipelineWorker_InitThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  // Filter
  @filterManagerInit(&state.filter)
  @filterManagerInsertFilter(&state.filter, _1_Lt500, _1_Lt500_Vec)
  @filterManagerFinalize(&state.filter)
  // Join hash table
  @joinHTInit(&state.jht, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
}

fun _1_pipelineWorker_TearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @filterManagerFree(&state.filter)
  @joinHTFree(&state.jht)
}

fun _1_pipelineWorker(queryState: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var filter = &state.filter
  var jht = &state.jht
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetVPI(tvi)

    // Filter on colA
    @filtersRun(filter, vec)

    // Insert into JHT using colB as key
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var key = @vpiGetInt(vec, 1)
      var elem = @ptrCast(*BuildRow, @joinHTInsert(jht, @hash(key)))
      elem.key = key
    }

    @vpiResetFiltered(vec)
  }
  return
}

fun checkKey(ctx: *int8, vec: *VectorProjectionIterator, tuple: *BuildRow) -> bool {
  return @vpiGetInt(vec, 1) == tuple.key
}

fun _2_pipelineWorker_InitThreadState(execCtx: *ExecutionContext, state: *ThreadState_2) -> nil {
  state.num_matches = 0
}

fun _2_pipelineWorker_TearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_2) -> nil { }

fun _2_pipelineWorker(queryState: *State, state: *ThreadState_2, tvi: *TableVectorIterator) -> nil {
  var jht = &queryState.jht
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetVPI(tvi)

    var key = @vpiGetInt(vec, 1)
    var hash_val = @hash(key)

    var iter: HashTableEntryIterator
    for (@joinHTLookup(&queryState.jht, &iter, hash_val);
         @htEntryIterHasNext(&iter, checkKey, queryState, vec);) {
      var unused = @htEntryIterGetRow(&iter)
      state.num_matches = state.num_matches + 1
    }

    @vpiReset(vec)
  }
}

fun pipeline2_finalize(qs: *State, ts: *ThreadState_2) -> nil {
  qs.num_matches = qs.num_matches + ts.num_matches
}

fun main(execCtx: *ExecutionContext) -> int {
  var state: State
  setUpState(execCtx, &state)

  // -------------------------------------------------------
  // Pipeline 1 - Begin
  // -------------------------------------------------------

  // Setup thread state container
  var tls: ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState_1), _1_pipelineWorker_InitThreadState, _1_pipelineWorker_TearDownThreadState, execCtx)

  // Parallel scan
  @iterateTableParallel("test_1", &state, &tls, _1_pipelineWorker)

  // -------------------------------------------------------
  // Pipeline 1 - Post-End
  // -------------------------------------------------------

  var off: uint32 = 0
  @joinHTBuildParallel(&state.jht, &tls, off)

  // -------------------------------------------------------
  // Pipeline 2 - Begin
  // -------------------------------------------------------

  // Setup thread-local state and parallel scan for probe phase
  @tlsReset(&tls, @sizeOf(ThreadState_2), _2_pipelineWorker_InitThreadState, _2_pipelineWorker_TearDownThreadState, execCtx)
  @iterateTableParallel("test_1", &state, &tls, _2_pipelineWorker)

  // -------------------------------------------------------
  // Pipeline 2 - End
  // -------------------------------------------------------

  @tlsIterate(&tls, &state, pipeline2_finalize)

  var ret = state.num_matches

  // Cleanup
  @tlsFree(&tls)
  tearDownState(&state)

  return ret
}
