struct State {
  table: JoinHashTable
}

fun pipeline_1(state: *State) -> nil {
  for (vec in test_1@[batch=2048]) {
    tpl_ht_insert(&state.table, vec)
  }
}

fun pipeline_2(state: *State) -> nil {
  for (vec in test_1@[batch=1024]) { }
}

fun main() -> int32 {
  var state: State

  // Run pipeline 1
  pipeline_1(&state)

  // Build table
  tpl_ht_build(&state.table)
 
  // Run the second pipeline
  pipeline_2(&state)

  return 0
}
