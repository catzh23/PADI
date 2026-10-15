package didatrade.server;

import didatrade.DidaTradePaxos.*;
import didatrade.DidaTradePaxosServiceGrpc;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Deterministic RPC barriers: later slots must progress before earlier replies are released. */
public class PipelineChecks {
  static int failures;

  static void check(String name, boolean ok) {
    System.out.println((ok ? "PASS " : "FAIL ") + name);
    if (!ok) failures++;
  }

  static boolean await(BooleanSupplier condition) throws Exception {
    long end = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < end) {
      if (condition.getAsBoolean()) return true;
      Thread.sleep(10);
    }
    return false;
  }

  static class Peer extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
    final int id;
    final Map<Integer, AcceptedInstance> accepted = new HashMap<>();
    final List<PhaseTwoRequest> requests = new ArrayList<>();
    final Map<String, List<Runnable>> replies = new HashMap<>();
    final Set<Integer> held = new HashSet<>();
    boolean holdAll;
    int failSlot = -1;
    int promised;
    int prepares;

    Peer(int id) { this.id = id; }

    synchronized void seed(int slot, int value, int ballot) {
      accepted.put(slot, AcceptedInstance.newBuilder().setInstance(slot)
          .setValue(value).setValballot(ballot).build());
    }

    @Override
    public synchronized void phaseone(PhaseOneRequest request, StreamObserver<PhaseOneReply> out) {
      prepares++;
      boolean ok = request.getRequestballot() >= promised;
      if (ok) promised = request.getRequestballot();
      var reply = PhaseOneReply.newBuilder().setServerid(id).setAccepted(ok)
          .setRequestballot(request.getRequestballot()).setMaxballot(promised);
      if (ok) reply.addAllAcceptedinstances(accepted.values());
      out.onNext(reply.build());
      out.onCompleted();
    }

    @Override
    public synchronized void phasetwo(PhaseTwoRequest request, StreamObserver<PhaseTwoReply> out) {
      requests.add(request);
      if (request.getInstance() == failSlot) {
        out.onError(Status.UNAVAILABLE.asRuntimeException());
        return;
      }
      boolean ok = request.getRequestballot() >= promised;
      if (ok) {
        promised = request.getRequestballot();
        seed(request.getInstance(), request.getValue(), promised);
      }
      var reply = PhaseTwoReply.newBuilder().setServerid(id).setAccepted(ok)
          .setInstance(request.getInstance()).setRequestballot(request.getRequestballot())
          .setMaxballot(promised).build();
      Runnable deliver = () -> { out.onNext(reply); out.onCompleted(); };
      if (holdAll || held.contains(request.getInstance())) {
        replies.computeIfAbsent(request.getRequestballot() + ":" + request.getInstance(),
            key -> new ArrayList<>()).add(deliver);
      } else deliver.run();
    }

    synchronized void hold(int slot) { held.add(slot); }
    synchronized void fail(int slot) { failSlot = slot; }
    synchronized void release(int ballot, int slot) {
      var waiting = replies.remove(ballot + ":" + slot);
      if (waiting != null) waiting.forEach(Runnable::run);
    }
    synchronized long count(int ballot, int slot) {
      return requests.stream().filter(r -> r.getRequestballot() == ballot && r.getInstance() == slot).count();
    }
    synchronized List<PhaseTwoRequest> requests() { return List.copyOf(requests); }
    synchronized int prepares() { return prepares; }
  }

  static class Cluster implements AutoCloseable {
    final DidaTradeServerState state = new DidaTradeServerState(29380, 1, 'A');
    final List<Peer> peers = new ArrayList<>();
    final List<Server> servers = new ArrayList<>();

    Cluster() throws Exception {
      for (int id = 0; id < 3; id++) {
        var peer = new Peer(id);
        var server = ServerBuilder.forPort(0).addService(peer).build().start();
        state.channels[id].shutdownNow();
        state.channels[id] = ManagedChannelBuilder.forAddress("localhost", server.getPort())
            .usePlaintext().build();
        state.async_stubs[id] = DidaTradePaxosServiceGrpc.newStub(state.channels[id]);
        peers.add(peer);
        servers.add(server);
      }
    }

    void pending(int id, DidaTradeCommand command) {
      state.req_history.addToPending(id, new RequestRecord(id, command));
    }
    void ballot(int ballot) { state.setCurrentBallot(ballot); state.main_loop.wakeup(); }
    boolean processed(int id) { return state.req_history.getIfProcessed(id) != null; }
    boolean decided(int slot) {
      var entry = state.paxos_log.getEntry(slot);
      return entry != null && entry.decided;
    }
    public void close() throws Exception {
      state.main_loop_worker.interrupt();
      state.main_loop_worker.join(2000);
      for (var channel : state.channels) channel.shutdownNow();
      for (var server : servers) server.shutdownNow();
    }
  }

  static void orderingAndBallots() throws Exception {
    try (var c = new Cluster()) {
      for (var peer : c.peers) {
        peer.seed(0, 101, 0);
        peer.seed(1, 201, 0);
        peer.hold(0);
      }
      c.pending(101, new DidaTradeCommand(DidaTradeAction.ADDUSER, 77, 42, 10));
      var balance = new DidaTradeCommand(DidaTradeAction.BALANCE, 77);
      c.pending(201, balance);
      c.ballot(4);
      check("pipeline: slot 1 decides while every reply for slot 0 is held",
          await(() -> c.decided(1)) && !c.decided(0));
      check("pipeline: later decision does not execute or answer before slot 0",
          !c.processed(101) && !c.processed(201) && c.state.trade_manager.balance(77) == -1);
      for (var peer : c.peers) peer.release(4, 0);
      check("pipeline: execution remains ordered (add user before reading balance)",
          await(() -> c.processed(201)) && c.processed(101) && balance.getQuantity() == 42);
      check("pipeline: overlapping slots share one prepare",
          await(() -> c.peers.stream().allMatch(p -> p.prepares() == 1)));

      for (var peer : c.peers) peer.hold(2);
      c.pending(301, new DidaTradeCommand(DidaTradeAction.BALANCE, 0));
      c.state.main_loop.wakeup();
      check("pipeline: old ballot has a proposal in flight",
          await(() -> c.peers.stream().allMatch(p -> p.count(4, 2) == 1)));
      c.ballot(7);
      check("pipeline: ballot change prepares and reproposes without waiting for old replies",
          await(() -> c.peers.stream().allMatch(p -> p.count(7, 2) == 1)));
      for (var peer : c.peers) peer.release(4, 2);
      Thread.sleep(350);
      check("pipeline: old callbacks cannot complete the new ballot's attempt",
          !c.decided(2) && !c.processed(301));
      for (var peer : c.peers) peer.release(7, 2);
      check("pipeline: recovered proposal completes in new ballot", await(() -> c.processed(301)));

      c.peers.get(1).fail(3);
      c.peers.get(2).fail(3);
      c.pending(401, new DidaTradeCommand(DidaTradeAction.BALANCE, 0));
      c.state.main_loop.wakeup();
      check("pipeline: failed RPC round retries without declaring a decision",
          await(() -> c.peers.get(0).count(7, 3) >= 2) && !c.decided(3) && !c.processed(401));
      c.pending(501, new DidaTradeCommand(DidaTradeAction.BALANCE, 0));
      c.state.main_loop.wakeup();
      check("pipeline: another slot progresses while the failed slot is retried",
          await(() -> c.decided(4)) && !c.processed(501));
      check("pipeline: retries keep the same value for the slot and ballot",
          c.peers.get(0).requests().stream().filter(r -> r.getInstance() == 3)
              .allMatch(r -> r.getValue() == 401));
      c.peers.get(1).fail(-1);
      c.peers.get(2).fail(-1);
      check("pipeline: recovery of RPCs unblocks ordered execution",
          await(() -> c.processed(501)) && c.processed(401));
    }
  }

  static void boundedWindow() throws Exception {
    try (var c = new Cluster()) {
      for (var peer : c.peers) peer.holdAll = true;
      for (int id = 101; id <= 140; id++)
        c.pending(id, new DidaTradeCommand(DidaTradeAction.BALANCE, 0));
      c.ballot(4);
      check("pipeline: fills the window without waiting for any phase 2 response",
          await(() -> c.peers.stream().allMatch(p -> p.requests().size() == MainLoop.PIPELINE_WINDOW)));
      Thread.sleep(350);
      var requests = c.peers.get(0).requests();
      check("pipeline: window bounds outstanding slots", requests.size() == MainLoop.PIPELINE_WINDOW);
      check("pipeline: each pending request is assigned to only one slot",
          requests.stream().map(PhaseTwoRequest::getValue).distinct().count() == requests.size());
      for (var peer : c.peers) peer.release(4, 0);
      check("pipeline: executing the first slot opens one place in the window",
          await(() -> c.peers.stream().allMatch(p -> p.count(4, MainLoop.PIPELINE_WINDOW) == 1)));
    }
  }

  public static void main(String[] args) {
    try {
      orderingAndBallots();
      boundedWindow();
    } catch (Throwable error) {
      error.printStackTrace();
      failures++;
    }
    System.out.println("Failed checks: " + failures);
    System.exit(failures == 0 ? 0 : 1);
  }
}
