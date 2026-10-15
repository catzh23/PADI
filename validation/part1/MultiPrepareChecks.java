package didatrade.server;

import didatrade.DidaTradePaxos.*;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.configs.ConfigurationScheduler;
import didatrade.util.PhaseOneResponseProcessor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Standalone regression checks; run with run.py multiprepare. */
public class MultiPrepareChecks {
  private static int failures;

  private static void check(String name, boolean ok) {
    System.out.println((ok ? "PASS " : "FAIL ") + name);
    if (!ok) failures++;
  }

  private static AcceptedInstance accepted(int slot, int value, int ballot) {
    return AcceptedInstance.newBuilder().setInstance(slot).setValue(value)
        .setValballot(ballot).build();
  }

  private static PhaseOneReply promise(int server, AcceptedInstance... entries) {
    return PhaseOneReply.newBuilder().setServerid(server).setAccepted(true)
        .setRequestballot(4).addAllAcceptedinstances(Arrays.asList(entries)).build();
  }

  private static void processors() {
    for (boolean reverse : new boolean[]{false, true}) {
      var p = new PhaseOneResponseProcessor(new ConfigurationScheduler('A'), 0, 4);
      var replies = new ArrayList<PhaseOneReply>();
      var a = promise(0, accepted(7, 101, 1), accepted(8, 201, 3));
      var b = promise(2, accepted(7, 102, 2), accepted(8, 202, 1), accepted(10, 301, 0));
      p.onNext(replies, reverse ? b : a);
      check("multi-prepare: one snapshot is not quorum", !p.getAccepted());
      p.onNext(replies, reverse ? a : b);
      var values = p.getAcceptedInstances();
      check("multi-prepare: highest ballot independently per slot, reverse=" + reverse,
          p.getAccepted() && values.get(7).getValue() == 102
              && values.get(8).getValue() == 201 && values.get(10).getValue() == 301
              && !values.containsKey(9));
    }
    var p = new PhaseOneResponseProcessor(new ConfigurationScheduler('A'), 0, 4);
    var replies = new ArrayList<PhaseOneReply>();
    p.onNext(replies, promise(0, accepted(7, 101, 1)));
    p.onNext(replies, promise(0, accepted(8, 201, 2)));
    check("multi-prepare: duplicate snapshot neither counts nor changes recovery",
        !p.getAccepted() && !p.getAcceptedInstances().containsKey(8));
    p.onNext(replies, PhaseOneReply.newBuilder().setServerid(1).setAccepted(false)
        .setMaxballot(5).build());
    check("multi-prepare: rejected preparation cannot authorize skip", !p.getAccepted());
  }

  private static class Reply<T> implements StreamObserver<T> {
    T value;
    public void onNext(T value) { this.value = value; }
    public void onError(Throwable error) { throw new AssertionError(error); }
    public void onCompleted() {}
  }

  private static PhaseTwoReply accept(DidaTradePaxosServiceImpl service, int slot, int value, int ballot) {
    var reply = new Reply<PhaseTwoReply>();
    service.phasetwo(PhaseTwoRequest.newBuilder().setInstance(slot).setValue(value)
        .setRequestballot(ballot).build(), reply);
    return reply.value;
  }

  private static void acceptor() throws Exception {
    // S2 is not leader in ballots 0, 1, 4 or 7, so its worker remains a learner.
    var state = new DidaTradeServerState(29180, 2, 'A');
    var service = new DidaTradePaxosServiceImpl(state);
    accept(service, 7, 101, 0);
    accept(service, 8, 201, 0);
    service.learn(LearnRequest.newBuilder().setInstance(7).setBallot(1).setValue(999).build(),
        new Reply<LearnReply>());
    var reply = new Reply<PhaseOneReply>();
    service.phaseone(PhaseOneRequest.newBuilder().setInstance(0).setRequestballot(4).build(), reply);
    var values = new HashMap<Integer, AcceptedInstance>();
    for (var entry : reply.value.getAcceptedinstancesList()) values.put(entry.getInstance(), entry);
    check("acceptor: prepare slot 0 returns accepted slots 7 and 8",
        reply.value.getAccepted() && values.size() == 2 && values.get(7).getValue() == 101
            && values.get(8).getValue() == 201);
    check("acceptor: learner messages do not change accepted value/ballot",
        values.get(7).getValballot() == 0 && values.get(7).getValue() == 101);
    check("acceptor: global promise rejects lower ballot in unseen slot",
        !accept(service, 20, 401, 3).getAccepted());
    check("acceptor: prepared ballot can accept unseen slot",
        accept(service, 20, 401, 4).getAccepted());
    check("acceptor: previous prepare response is an immutable snapshot",
        reply.value.getAcceptedinstancesCount() == 2);
    var suffix = new Reply<PhaseOneReply>();
    service.phaseone(PhaseOneRequest.newBuilder().setInstance(8).setRequestballot(4).build(), suffix);
    check("acceptor: lower bound includes slot 8 and future slot 20 but excludes slot 7",
        suffix.value.getAccepted()
            && suffix.value.getAcceptedinstancesCount() == 2
            && suffix.value.getAcceptedinstancesList().stream()
                .allMatch(e -> e.getInstance() == 8 || e.getInstance() == 20));
    service.phaseone(PhaseOneRequest.newBuilder().setInstance(21).setRequestballot(4).build(), suffix);
    check("acceptor: no accepted slots in requested suffix returns an empty list",
        suffix.value.getAccepted() && suffix.value.getAcceptedinstancesCount() == 0);
    check("acceptor: suffix filtering does not weaken the global promise",
        !accept(service, 7, 999, 3).getAccepted());
    pending(state, 501);
    for (int slot = 0; slot < 2; slot++) {
      var learned = LearnRequest.newBuilder().setInstance(slot).setBallot(4)
          .setValue(slot == 0 ? 0 : 501).build();
      service.learn(learned, new Reply<LearnReply>());
      service.learn(learned, new Reply<LearnReply>());
      check("learner: duplicate acceptor cannot decide slot " + slot,
          !state.paxos_log.getEntry(slot).decided);
      service.learn(learned.toBuilder().setServerid(1).build(), new Reply<LearnReply>());
    }
    check("learner: skips decided no-op and executes following command",
        await(() -> state.req_history.getIfProcessed(501) != null));
    service.learn(LearnRequest.newBuilder().setInstance(1).setBallot(1).setValue(999).build(),
        new Reply<LearnReply>());
    check("learner: late notification cannot overwrite decided command", chosen(state, 1) == 501);
    for (var channel : state.channels) channel.shutdownNow();
  }

  /** Controlled RPC peers; tests below exercise the real MainLoop over gRPC. */
  private static class Peer extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
    final int id;
    final Map<Integer, AcceptedInstance> log = new HashMap<>();
    int prepares;
    int promised;

    Peer(int id) { this.id = id; }

    synchronized void seed(int slot, int value, int ballot) {
      log.put(slot, accepted(slot, value, ballot));
    }

    public synchronized void phaseone(PhaseOneRequest request, StreamObserver<PhaseOneReply> out) {
      prepares++;
      boolean ok = request.getRequestballot() >= promised;
      if (ok) promised = request.getRequestballot();
      var response = PhaseOneReply.newBuilder().setServerid(id).setAccepted(ok)
          .setRequestballot(request.getRequestballot()).setMaxballot(promised);
      if (ok) response.addAllAcceptedinstances(log.values());
      out.onNext(response.build());
      out.onCompleted();
    }

    public synchronized void phasetwo(PhaseTwoRequest request, StreamObserver<PhaseTwoReply> out) {
      boolean ok = request.getRequestballot() >= promised;
      if (ok) {
        promised = request.getRequestballot();
        seed(request.getInstance(), request.getValue(), promised);
      }
      out.onNext(PhaseTwoReply.newBuilder().setServerid(id).setAccepted(ok)
          .setRequestballot(request.getRequestballot()).setMaxballot(promised).build());
      out.onCompleted();
    }

    synchronized int prepares() { return prepares; }
  }

  private static boolean await(BooleanSupplier condition) throws Exception {
    long until = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < until) {
      if (condition.getAsBoolean()) return true;
      Thread.sleep(10);
    }
    return false;
  }

  private static void pending(DidaTradeServerState state, int id) {
    state.req_history.addToPending(id,
        new RequestRecord(id, new DidaTradeCommand(DidaTradeAction.BALANCE, 0)));
  }

  private static int chosen(DidaTradeServerState state, int slot) {
    var entry = state.paxos_log.getEntry(slot);
    return entry != null && entry.decided ? entry.command_id : -1;
  }

  private static void leader() throws Exception {
    var state = new DidaTradeServerState(29280, 1, 'A');
    var peers = new ArrayList<Peer>();
    var servers = new ArrayList<io.grpc.Server>();
    for (int id = 0; id < 3; id++) {
      var peer = new Peer(id);
      peer.seed(1, 101, 0);
      peer.seed(3, 301, 3);
      var server = ServerBuilder.forPort(0).addService(peer).build().start();
      state.channels[id].shutdownNow();
      state.channels[id] = ManagedChannelBuilder.forAddress("localhost", server.getPort())
          .usePlaintext().build();
      state.async_stubs[id] = DidaTradePaxosServiceGrpc.newStub(state.channels[id]);
      peers.add(peer);
      servers.add(server);
    }
    // Both pending IDs are reserved for later slots. Holes must not steal them.
    pending(state, 101);
    pending(state, 301);
    state.setCurrentBallot(4);
    state.main_loop.wakeup();
    check("leader: recovers later slots and executes their requests",
        await(() -> state.req_history.getIfProcessed(301) != null));
    check("leader: preserves recovered values and fills holes with no-op",
        chosen(state, 0) == 0 && chosen(state, 1) == 101
            && chosen(state, 2) == 0 && chosen(state, 3) == 301);
    pending(state, 401);
    state.main_loop.wakeup();
    check("leader: new request progresses in same ballot",
        await(() -> state.req_history.getIfProcessed(401) != null) && chosen(state, 4) == 401);
    await(() -> peers.stream().allMatch(p -> p.prepares() >= 1));
    check("leader: only one prepare per peer for five slots",
        peers.stream().allMatch(p -> p.prepares() == 1));
    for (var peer : peers) peer.seed(6, 601, 4);
    state.setCurrentBallot(7);
    state.main_loop.wakeup();
    check("leader: recovers without a new pending client request",
        await(() -> chosen(state, 6) == 601));
    pending(state, 601);
    state.main_loop.wakeup();
    check("leader: new ballot recovers newly accepted future slot",
        await(() -> state.req_history.getIfProcessed(601) != null)
            && chosen(state, 5) == 0 && chosen(state, 6) == 601);
    await(() -> peers.stream().allMatch(p -> p.prepares() >= 2));
    check("leader: ballot change performs a fresh multi-prepare",
        peers.stream().allMatch(p -> p.prepares() == 2));
    for (var channel : state.channels) channel.shutdownNow();
    for (var server : servers) server.shutdownNow();
  }

  public static void main(String[] args) {
    try {
      processors();
      acceptor();
      leader();
    } catch (Throwable error) {
      error.printStackTrace();
      failures++;
    }
    System.out.println("Failed checks: " + failures);
    // Production workers have no shutdown API; this is a dedicated test process.
    System.exit(failures == 0 ? 0 : 1);
  }
}
