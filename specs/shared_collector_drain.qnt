// SharedCollector's ArcSwap generation handoff for one counter key.
// Two drain slots make overlapping SharedDrain values observable.
module shared_collector_drain {
  var s: {
    current: int,
    pending: int,
    drain1: int,
    drain2: int,
    emitted: int,
    recorded: int,
    currentWriters: int,
    pendingWriters: int,
    hasPending: bool,
    drain1Open: bool,
    drain2Open: bool,
    currentGen: int,
    pendingGen: int,
    drain1Gen: int,
    drain2Gen: int,
  }

  // -1 means that a pending or drain slot owns no generation.
  action init = s' = {
    current: 0, pending: 0, drain1: 0, drain2: 0,
    emitted: 0, recorded: 0,
    currentWriters: 0, pendingWriters: 0,
    hasPending: false, drain1Open: false, drain2Open: false,
    currentGen: 0, pendingGen: -1, drain1Gen: -1, drain2Gen: -1,
  }

  // load() keeps a guard to the current Arc until recording finishes.
  action startWrite = all {
    s.currentWriters < 2,
    s' = { ...s, currentWriters: s.currentWriters + 1 },
  }

  action finishCurrentWrite = all {
    s.currentWriters > 0,
    s' = {
      ...s,
      current: s.current + 1,
      recorded: s.recorded + 1,
      currentWriters: s.currentWriters - 1,
    },
  }

  // A guard loaded before swap() still refers to the detached Arc.
  action finishPendingWrite = all {
    s.hasPending,
    s.pendingWriters > 0,
    s' = {
      ...s,
      pending: s.pending + 1,
      recorded: s.recorded + 1,
      pendingWriters: s.pendingWriters - 1,
    },
  }

  // begin_drain() swaps only when there is no pending Arc. It does not wait
  // for an earlier SharedDrain to be dropped; it can allocate a fresh buffer.
  action swapGeneration = all {
    not(s.hasPending),
    s' = {
      ...s,
      current: 0,
      currentWriters: 0,
      currentGen: s.currentGen + 1,
      pending: s.current,
      pendingWriters: s.currentWriters,
      pendingGen: s.currentGen,
      hasPending: true,
    },
  }

  // try_unwrap() fails and begin_drain() returns None. The pending Arc stays.
  action retryBlockedDrain = all {
    s.hasPending,
    s.pendingWriters > 0,
    s' = s,
  }

  // On a successful try_unwrap(), each SharedDrain owns its detached buffer.
  action openDrain1 = all {
    s.hasPending,
    s.pendingWriters == 0,
    not(s.drain1Open),
    s' = {
      ...s,
      pending: 0,
      pendingGen: -1,
      hasPending: false,
      drain1: s.pending,
      drain1Gen: s.pendingGen,
      drain1Open: true,
    },
  }

  action openDrain2 = all {
    s.hasPending,
    s.pendingWriters == 0,
    not(s.drain2Open),
    s' = {
      ...s,
      pending: 0,
      pendingGen: -1,
      hasPending: false,
      drain2: s.pending,
      drain2Gen: s.pendingGen,
      drain2Open: true,
    },
  }

  // A complete frames() traversal takes the count; Drop recycles the buffer.
  action emitAndCloseDrain1 = all {
    s.drain1Open,
    s' = {
      ...s,
      emitted: s.emitted + s.drain1,
      drain1: 0,
      drain1Gen: -1,
      drain1Open: false,
    },
  }

  action emitAndCloseDrain2 = all {
    s.drain2Open,
    s' = {
      ...s,
      emitted: s.emitted + s.drain2,
      drain2: 0,
      drain2Gen: -1,
      drain2Open: false,
    },
  }

  action step = any {
    startWrite,
    finishCurrentWrite,
    finishPendingWrite,
    swapGeneration,
    retryBlockedDrain,
    openDrain1,
    openDrain2,
    emitAndCloseDrain1,
    emitAndCloseDrain2,
  }

  // Completed writes remain in a generation or have been emitted once.
  val noLostOrDuplicatedCount =
    s.recorded == s.current + s.pending + s.drain1 + s.drain2 + s.emitted

  val noEarlyDrain =
    s.pendingWriters > 0 implies
      (s.hasPending and s.pendingGen != s.drain1Gen and s.pendingGen != s.drain2Gen)

  val distinctOwners =
    (not(s.hasPending) or s.pendingGen != s.currentGen) and
    (not(s.drain1Open) or s.drain1Gen != s.currentGen) and
    (not(s.drain2Open) or s.drain2Gen != s.currentGen) and
    (not(s.hasPending and s.drain1Open) or s.pendingGen != s.drain1Gen) and
    (not(s.hasPending and s.drain2Open) or s.pendingGen != s.drain2Gen) and
    (not(s.drain1Open and s.drain2Open) or s.drain1Gen != s.drain2Gen)

  val emptySlots =
    (s.hasPending or (s.pending == 0 and s.pendingWriters == 0 and s.pendingGen == -1)) and
    (s.drain1Open or (s.drain1 == 0 and s.drain1Gen == -1)) and
    (s.drain2Open or (s.drain2 == 0 and s.drain2Gen == -1))

  val nonnegative =
    s.current >= 0 and s.pending >= 0 and s.drain1 >= 0 and s.drain2 >= 0 and
    s.emitted >= 0 and s.recorded >= 0 and
    s.currentWriters >= 0 and s.pendingWriters >= 0

  val safe = noLostOrDuplicatedCount and noEarlyDrain and
    distinctOwners and emptySlots and nonnegative

  run writeAcrossSwapTest = init
    .then(startWrite)
    .then(swapGeneration)
    .then(retryBlockedDrain)
    .then(finishPendingWrite)
    .then(openDrain1)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 1 and safe)

  run newWriteWhileOldGenerationWaitsTest = init
    .then(startWrite)
    .then(swapGeneration)
    .then(startWrite)
    .then(finishCurrentWrite)
    .then(finishPendingWrite)
    .then(openDrain1)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 1 and s.current == 1 and safe)

  run overlappingDrainsTest = init
    .then(startWrite)
    .then(finishCurrentWrite)
    .then(swapGeneration)
    .then(openDrain1)
    .then(startWrite)
    .then(finishCurrentWrite)
    .then(swapGeneration)
    .then(openDrain2)
    .then(emitAndCloseDrain2)
    .then(emitAndCloseDrain1)
    .expect(s.emitted == 2 and safe)

  run pendingWithEarlierDrainOpenTest = init
    .then(swapGeneration)
    .then(openDrain1)
    .then(startWrite)
    .then(swapGeneration)
    .then(retryBlockedDrain)
    .then(finishPendingWrite)
    .then(openDrain2)
    .expect(s.drain1Open and s.drain2Open and s.drain2 == 1 and safe)
}
