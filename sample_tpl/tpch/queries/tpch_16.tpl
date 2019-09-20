struct OutputStruct {
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
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
}


fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  pipeline1(execCtx, state)
  @outputFinalize(execCtx)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}
