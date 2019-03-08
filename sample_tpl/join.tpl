struct State {
  table: map[int32]int32
}

fun pipeline_1(state: *State) -> nil {
  for (vec in test_1@[batch=2048]) {
    //insert(state.table, vec, "colA", ["colB", "colC"])
  }
}

fun pipeline_2(state: *State) -> nil {
  for (vec in test_1@[batch=1024]) {
  }
}

fun main() -> int32 {
  var state: State
  pipeline_1(&state)
  pipeline_2(&state)
  return 0
}
