struct State {
  table: JoinHashTable
  num_matches: int32
}

struct BuildRow {
  key: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  // Complex bits
  @joinHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  // Simple bits
  state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.table)
}

fun pipeline_1(state: *State) -> nil {
  var jht: *JoinHashTable = &state.table

  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)

    var key = @vpiGetInt(vec, 1)
    var hash_val = @hash(key)
    var elem = @ptrCast(*BuildRow, @joinHTInsert(jht, hash_val))
    elem.key = key

    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun checkKey(ctx: *int8, vec: *VectorProjectionIterator, tuple: *BuildRow) -> bool {
  return @vpiGetInt(vec, 1) == tuple.key
}

fun pipeline_2(state: *State) -> nil {
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)

    var key = @vpiGetInt(vec, 1)
    var hash_val = @hash(key)

    var iter: HashTableEntryIterator
    for (@joinHTLookup(&state.table, &iter, hash_val); @htEntryIterHasNext(&iter); ) {
      var build_row = @ptrCast(*BuildRow, @htEntryIterGetRow(&iter))
      if (build_row.key == @vpiGetInt(vec, 1)) {
        state.num_matches = state.num_matches + 1
      }
    }

    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(&state)

  // Build table
  @joinHTBuild(&state.table)
 
  // Run the second pipeline
  pipeline_2(&state)

  var ret = state.num_matches

  // Cleanup
  tearDownState(&state)

  return ret
}
