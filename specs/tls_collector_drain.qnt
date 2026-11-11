// Two producer threads, one counter key, and up to two open TLSDrain values.
// Each begin_drain() swaps/merges local buffers into one detached generation.
module tls_collector_drain {
  var s: {
    undrained: int,
    drain1: int,
    drain2: int,
    recorded: int,
    removed: int,
    emitted: int,
    drain1Open: bool,
    drain2Open: bool,
    nextGen: int,
    drain1Gen: int,
    drain2Gen: int,
  }

  action init = s' = {
    undrained: 0, drain1: 0, drain2: 0,
    recorded: 0, removed: 0, emitted: 0,
    drain1Open: false, drain2Open: false,
    nextGen: 0, drain1Gen: -1, drain2Gen: -1,
  }

  // The actions have separate thread identities so replay uses real threads.
  action addThread0 = s' = {
    ...s, undrained: s.undrained + 1, recorded: s.recorded + 1,
  }

  action addThread1 = s' = {
    ...s, undrained: s.undrained + 1, recorded: s.recorded + 1,
  }

  // A signed counter decrement is recorded in a live local buffer.
  // It cannot change a detached TLSDrain, and the net may be negative.
  action subtractOne = s' = {
    ...s,
    undrained: s.undrained - 1,
    removed: s.removed + 1,
  }

  action openDrain1 = all {
    not(s.drain1Open),
    s' = {
      ...s,
      undrained: 0,
      drain1: s.undrained,
      drain1Open: true,
      drain1Gen: s.nextGen,
      nextGen: s.nextGen + 1,
    },
  }

  action openDrain2 = all {
    not(s.drain2Open),
    s' = {
      ...s,
      undrained: 0,
      drain2: s.undrained,
      drain2Open: true,
      drain2Gen: s.nextGen,
      nextGen: s.nextGen + 1,
    },
  }

  action emitAndCloseDrain1 = all {
    s.drain1Open,
    s' = {
      ...s,
      emitted: s.emitted + s.drain1,
      drain1: 0,
      drain1Open: false,
      drain1Gen: -1,
    },
  }

  action emitAndCloseDrain2 = all {
    s.drain2Open,
    s' = {
      ...s,
      emitted: s.emitted + s.drain2,
      drain2: 0,
      drain2Open: false,
      drain2Gen: -1,
    },
  }

  action step = any {
    addThread0,
    addThread1,
    subtractOne,
    openDrain1,
    openDrain2,
    emitAndCloseDrain1,
    emitAndCloseDrain2,
  }

  val conserved =
    s.recorded == s.undrained + s.drain1 + s.drain2 + s.emitted + s.removed

  val detachedUntouched =
    (s.drain1Open or (s.drain1 == 0 and s.drain1Gen == -1)) and
    (s.drain2Open or (s.drain2 == 0 and s.drain2Gen == -1)) and
    (not(s.drain1Open and s.drain2Open) or s.drain1Gen != s.drain2Gen)

  val nonnegative = s.recorded >= 0 and s.removed >= 0

  val safe = conserved and detachedUntouched and nonnegative

  run twoThreadsMergeTest = init
    .then(addThread0)
    .then(addThread1)
    .then(openDrain1)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 2 and safe)

  run subtractAcrossThreadsTest = init
    .then(addThread0)
    .then(addThread1)
    .then(subtractOne)
    .then(openDrain1)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 1 and s.removed == 1 and safe)

  run subtractCannotChangeOpenDrainTest = init
    .then(addThread0)
    .then(openDrain1)
    .then(subtractOne)
    .then(addThread1)
    .then(subtractOne)
    .then(openDrain2)
    .then(emitAndCloseDrain2)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 0 and s.removed == 2 and safe)

  run overlappingGenerationsTest = init
    .then(addThread0)
    .then(openDrain1)
    .then(addThread1)
    .then(openDrain2)
    .then(emitAndCloseDrain2)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 2 and safe)
}
