struct State {
    jht         : JoinHashTable
    num_matches : uint32
}

struct ThreadState_1 {
    jht            : JoinHashTable
    filter_manager : FilterManager
}

struct ThreadState_2 {
    num_matches: uint32
}

struct BuildRow {
    key: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    // JoinHashTable initialization
    @joinHTInit(&state.jht, @execCtxGetMem(execCtx), @sizeOf(BuildRow))

    // Simple bits
    state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
    @joinHTFree(&state.jht)
}

fun pipeline1_filter_clause0term0(vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    @filterLt(vector_proj, 0, @intToSql(500), tids)
}

fun pipeline1_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
    // Filter
    @filterManagerInit(&state.filter_manager)
    @filterManagerInsertFilter(&state.filter_manager, pipeline1_filter_clause0term0)
    @filterManagerFinalize(&state.filter_manager)

    // Join hash table
    @joinHTInit(&state.jht, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
}

fun pipeline1_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
    @filterManagerFree(&state.filter_manager)
    @joinHTFree(&state.jht)
}

fun pipeline1_worker(queryState: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
    var filter = &state.filter_manager
    var jht = &state.jht
    for (@tableIterAdvance(tvi)) {
        var vec = @tableIterGetVPI(tvi)

        // Filter on colA
        @filterManagerRunFilters(filter, vec)

        // Insert into JHT using colB as key
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var key = @vpiGetInt(vec, 1)
            var elem = @ptrCast(*BuildRow, @joinHTInsert(jht, @hash(key)))
            elem.key = key
        }

        @vpiResetFiltered(vec)
    }
}

fun checkKey(ctx: *int8, vec: *VectorProjectionIterator, tuple: *BuildRow) -> bool {
    return @vpiGetInt(vec, 1) == tuple.key
}

fun pipeline2_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_2) -> nil {
    state.num_matches = 0
}

fun pipeline2_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_2) -> nil { }

fun pipeline2_worker(queryState: *State, state: *ThreadState_2, tvi: *TableVectorIterator) -> nil {
    var jht = &queryState.jht
    for (@tableIterAdvance(tvi)) {
        var vec = @tableIterGetVPI(tvi)

        var key = @vpiGetInt(vec, 1)
        var hash_val = @hash(key)

        var iter: HashTableEntryIterator
        for (@joinHTLookup(&queryState.jht, &iter, hash_val); @htEntryIterHasNext(&iter, checkKey, queryState, vec);) {
            var unused = @htEntryIterGetRow(&iter)
            state.num_matches = state.num_matches + 1
        }

        @vpiReset(vec)
    }
}

fun pipeline2_finalize(qs: *State, ts: *ThreadState_2) -> nil {
    qs.num_matches = qs.num_matches + ts.num_matches
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls: ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(ThreadState_1), pipeline1_worker_initThreadState, pipeline1_worker_tearDownThreadState, execCtx)

    // Parallel scan "test_1"
    @iterateTableParallel("test_1", state, &tls, pipeline1_worker)

    // Parallel build the join hash table
    var off: uint32 = 0
    @joinHTBuildParallel(&state.jht, &tls, off)

    // Cleanup
    @tlsFree(&tls)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls: ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(ThreadState_2), pipeline2_worker_initThreadState, pipeline2_worker_tearDownThreadState, execCtx)

    // Parallel scan "test_1" again
    @iterateTableParallel("test_1", state, &tls, pipeline2_worker)

    // Collect results
    @tlsIterate(&tls, state, pipeline2_finalize)

    // Cleanup
    @tlsFree(&tls)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    var ret = state.num_matches
    tearDownState(&state)

    return ret
}
