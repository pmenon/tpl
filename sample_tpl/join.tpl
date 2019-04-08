struct State {
  alloc: RegionAlloc
  table: JoinHashTable
}

fun init_state(state: *State) -> nil {
  // Initialize allocator
  tpl_region_init(&state.alloc)

  // Initialize the join hash table
  tpl_ht_init(&state.table, &state.alloc, 10)
}

fun cleanup_state(state: *State) -> nil {
  tpl_region_free(&state.alloc)
}

fun pipeline_1(state: *State) -> nil {
  var jht: *JoinHashTable = &state.table
  for (vec in test_1@[batch=2048]) {
    tpl_ht_insert(jht, vec)
  }
}

fun pipeline_2(state: *State) -> nil {
  for (vec in test_1@[batch=1024]) { }
}

fun main() -> int32 {
  var state: State

  // Initialize state
  init_state(&state)

  // Run pipeline 1
  pipeline_1(&state)

  // Build table
  tpl_ht_build(&state.table)
 
  // Run the second pipeline
  pipeline_2(&state)

  // Cleanup
  cleanup_state(&state)

  return 0
}
