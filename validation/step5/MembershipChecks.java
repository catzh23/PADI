package didatrade.server;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMain;
import didatrade.DidaTradeMainServiceGrpc;
import didatrade.DidaTradeMasterServiceGrpc;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Real RPCs with a controlled initial history, including replicas missing request bodies. */
public class MembershipChecks {
  static void check(String name, boolean ok) {
    if (!ok) throw new AssertionError(name);
    System.out.println("PASS " + name);
  }

  static void await(String name, BooleanSupplier condition) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(10);
    check(name, condition.getAsBoolean());
  }

  static class Cluster implements AutoCloseable {
    final DidaTradeServerState[] states = new DidaTradeServerState[7];
    final Server[] servers = new Server[7];

    Cluster(int port) throws Exception {
      for (int i = 0; i < 7; i++) {
        states[i] = new DidaTradeServerState(port, i, 'C');
        states[i].main_loop_worker.interrupt();
        states[i].main_loop_worker.join(3000);
        check("initial worker stopped on S" + i, !states[i].main_loop_worker.isAlive());
        states[i].setCurrentBallot(2);
      }
    }

    void start() throws Exception {
      for (int i = 0; i < 7; i++) {
        servers[i] = ServerBuilder.forPort(states[i].base_port + i)
            .addService(new DidaTradePaxosServiceImpl(states[i]))
            .addService(new DidaTradeMainServiceImpl(states[i]))
            .addService(new DidaTradeMasterServiceImpl(states[i])).build().start();
      }
      for (var state : states) {
        state.main_loop_worker = new Thread(state.main_loop);
        state.main_loop_worker.start();
      }
    }

    DidaTradeMasterServiceGrpc.DidaTradeMasterServiceBlockingStub master(int replica) {
      return DidaTradeMasterServiceGrpc.newBlockingStub(states[0].channels[replica])
          .withDeadlineAfter(15, TimeUnit.SECONDS);
    }

    void prepare(int reference) {
      var reply = master(3).newballot(DidaTradeMaster.NewBallotRequest.newBuilder()
          .setNewballot(2).setCompletedballot(reference).build());
      check("preparation reports ballot 2 complete", reply.getCompletedballot() == 2);
    }

    public void close() throws Exception {
      for (var state : states) if (state != null) state.main_loop_worker.interrupt();
      for (var server : servers) if (server != null) server.shutdownNow();
      for (var state : states) if (state != null) {
        for (var channel : state.channels) channel.shutdownNow();
        state.main_loop_worker.join(3000);
      }
      for (var server : servers) if (server != null) server.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  static DidaTradeCommand sell(int user) {
    return new DidaTradeCommand(DidaTradeAction.SELL, user, 50);
  }

  static void pending(DidaTradeServerState state, int id, DidaTradeCommand command) {
    state.req_history.addToPending(id, new RequestRecord(id, command));
  }

  static void historyAndBodies(int port) throws Exception {
    try (var c = new Cluster(port)) {
      // Classic ballot 1 chose X and Y on S0/S1/S2. S5/S6 missed both requests.
      for (int i = 0; i < 3; i++) {
        pending(c.states[i], 101, sell(0));
        pending(c.states[i], 201, new DidaTradeCommand(DidaTradeAction.BUY, 1, 50));
        for (int slot : new int[] {0, 2}) {
          var entry = c.states[i].paxos_log.testAndSetEntry(slot);
          entry.accepted_value = slot == 0 ? 101 : 201;
          entry.write_ballot = 1;
        }
      }
      // The new leader already executed the history as a learner. Transfer is still required.
      var leader = c.states[3];
      pending(leader, 101, sell(0));
      pending(leader, 201, new DidaTradeCommand(DidaTradeAction.BUY, 1, 50));
      for (int slot = 0; slot < 3; slot++) {
        var entry = leader.paxos_log.testAndSetEntry(slot);
        entry.command_id = slot == 0 ? 101 : slot == 1 ? PaxosInstance.NO_OP : 201;
        entry.decided = true;
      }
      var execute = MainLoop.class.getDeclaredMethod("executeReadyEntries");
      execute.setAccessible(true);
      execute.invoke(leader.main_loop);
      check("leader already executed three slots", leader.getExecutedIndex() == 3);
      for (var state : c.states) pending(state, 301, sell(2));
      c.start();
      c.prepare(1);
      await("new learner executes transferred commands and hole", () -> c.states[5].getExecutedIndex() == 3);
      check("transferred request bodies restore balances",
          c.states[5].trade_manager.balance(0) == 15 && c.states[5].trade_manager.balance(1) == 5);
      var late_client = DidaTradeMainServiceGrpc.newBlockingStub(c.states[0].channels[5])
          .withDeadlineAfter(3, TimeUnit.SECONDS);
      var reply = late_client.sell(DidaTradeMain.SellRequest.newBuilder()
          .setReqid(101).setUserid(0).setQuantity(50).build());
      check("late client receives the transferred request's original response",
          reply.getResult() && reply.getLastindex() == 1 && c.states[5].getExecutedIndex() == 3);
      await("already executed slot is stored by the new acceptors", () -> {
        synchronized (c.states[6]) {
          var entry = c.states[6].paxos_log.getEntry(0);
          return entry != null && entry.write_ballot == 2 && entry.accepted_value == 101;
        }
      });
      check("preparation does not execute pending client request",
          leader.req_history.getIfProcessed(301) == null && leader.getAnyBallot() != 2);
      check("wrong ballot cannot be activated", !c.master(3).activate(
          DidaTradeMaster.ActivateRequest.newBuilder().setBallot(3).build()).getAck());
      check("prepared ballot can be activated", c.master(3).activate(
          DidaTradeMaster.ActivateRequest.newBuilder().setBallot(2).build()).getAck());
      await("Fast proposals resume after activation", () -> leader.getExecutedIndex() >= 4);
      var old = DidaTradePaxosServiceGrpc.newBlockingStub(c.states[0].channels[0])
          .withDeadlineAfter(3, TimeUnit.SECONDS);
      check("removed acceptor rejects new group writes", !old.phasetwo(
          DidaTradePaxos.PhaseTwoRequest.newBuilder().setRequestballot(2)
              .setInstance(10).setValue(999).build()).getAccepted());
      var current = DidaTradePaxosServiceGrpc.newBlockingStub(c.states[0].channels[2])
          .withDeadlineAfter(3, TimeUnit.SECONDS);
      check("old ballot cannot write after preparation", !current.phasetwo(
          DidaTradePaxos.PhaseTwoRequest.newBuilder().setRequestballot(1)
              .setInstance(10).setValue(999).build()).getAccepted());
    }
  }

  static void emptyHistory(int port) throws Exception {
    try (var c = new Cluster(port)) {
      c.start();
      c.prepare(0);
      check("empty history completes without a client command", c.states[3].getExecutedIndex() == 0);
      check("empty history still waits for activate", c.states[3].getAnyBallot() == -1);
      check("empty prepared ballot can activate", c.master(3).activate(
          DidaTradeMaster.ActivateRequest.newBuilder().setBallot(2).build()).getAck());
      c.states[3].setCurrentBallot(3);
      check("stale activate is rejected", !c.master(3).activate(
          DidaTradeMaster.ActivateRequest.newBuilder().setBallot(2).build()).getAck());
      check("Schedule C rejects ballot beyond its five configurations",
          !c.states[3].scheduler.validBallot(5));
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    historyAndBodies(port);
    emptyHistory(port + 10);
  }
}
