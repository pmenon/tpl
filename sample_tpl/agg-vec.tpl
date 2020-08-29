struct State {
    table: AggregationHashTable
    count: int32
}

struct Agg {
    key  : Integer
    count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
    state.count = 0
}

fun tearDownState(state: *State) -> nil {
    @aggHTFree(&state.table)
}

fun constructAggregates(new: *VectorProjectionIterator, input: *VectorProjectionIterator) -> nil {
    for (@vpiHasNext(new)) {
        var new_entry = @ptrCast(*HashTableEntryIterator, @vpiGetPointer(new, 1))
        var new_agg = @ptrCast(*Agg, @htEntryIterGetRow(new_entry))
        var input_key = @vpiGetInt(input, 1)
        new_agg.key = input_key
        @vpiAdvance(new)
        @vpiAdvance(input)
    }
}

fun advanceAggregates(new: *VectorProjectionIterator, input: *VectorProjectionIterator) -> nil {
    for (@vpiHasNext(new)) {
        var new_entry = @ptrCast(HashTableEntryIterator, @vpiGetPointer(new, 1))
        var new_agg = @ptrCast(*Agg, @htEntryIterGetRow(new_entry))
        var input_val = @vpiGetInt(input, 0)
        @aggAdvance(&new_agg.count, &input_val)
        @vpiAdvance(new)
        @vpiAdvance(input)
    }
}

fun pipeline_1(state: *State) -> nil {
    // The table
    var ht: *AggregationHashTable = &state.table

    // Setup the iterator and iterate
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
        var vec = @tableIterGetVPI(&tvi)
        var key_cols: [1]uint32
        key_cols[0] = 1
        @aggHTProcessBatch(ht, vec, key_cols, constructAggregates, advanceAggregates, false)
    }
    @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
    var aht_iter: AHTIterator
    var iter = &aht_iter
    for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
        var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
        state.count = state.count + 1
    }
    @aggHTIterClose(iter)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    // Initialize state
    setUpState(execCtx, &state)

    // Run pipeline 1
    pipeline_1(&state)

    // Run pipeline 2
    pipeline_2(&state)

    var ret = state.count

    // Cleanup
    tearDownState(&state)

    return ret
}
