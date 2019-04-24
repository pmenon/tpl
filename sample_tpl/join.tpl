struct State {
  alloc: RegionAlloc
  table: JoinHashTable
}

struct BuildRow {
  key: int32
}

fun setUpState(state: *State) -> nil {
  // Initialize the region
  @regionInit(&state.alloc)
  // Initialize the join hash table
  @joinHTInit(&state.table, &state.alloc, @sizeOf(BuildRow))
}

fun tearDownState(state: *State) -> nil {
  // Cleanup the join hash table
  @joinHTFree(&state.table)
  // Cleanup the region allocator
  @regionFree(&state.alloc)
}

fun pipeline_1(state: *State) -> nil {
  var jht: *JoinHashTable = &state.table
  var hash_val: uint64 = 10

  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)

    var elem: *BuildRow = @joinHTInsert(jht, hash_val)
    elem.key = 44

    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun main() -> int32 {
  var state: State

  // Initialize state
  setUpState(&state)

  // Run pipeline 1
  pipeline_1(&state)

  // Build table
  @joinHTBuild(&state.table)
 
  // Run the second pipeline
  pipeline_2(&state)

  // Cleanup
  tearDownState(&state)

  return 0
}
