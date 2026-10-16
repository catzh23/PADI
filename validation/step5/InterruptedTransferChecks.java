package didatrade.server;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.configs.ConfigurationScheduler;
import didatrade.util.PhaseOneResponseProcessor;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/** A slot may be decided during a transfer even though the whole transfer is unfinished. */
public class InterruptedTransferChecks {
  static void promise(PhaseOneResponseProcessor processor, int id) {
    processor.onNext(new ArrayList<>(), DidaTradePaxos.PhaseOneReply.newBuilder()
        .setServerid(id).setAccepted(true).build());
  }

  static void quorumChecks() {
    var schedule = new ConfigurationScheduler('C');
    for (int reference : new int[] {0, 1}) {
      for (int ballot : new int[] {3, 4}) {
        var processor = new PhaseOneResponseProcessor(schedule, reference, ballot);
        for (int id : new int[] {0, 1, 4}) promise(processor, id);
        MembershipChecks.check("old quorum alone cannot finish recovery " + reference + " -> " + ballot,
            !processor.getAccepted());
        promise(processor, 5);
        promise(processor, 5);
        MembershipChecks.check("duplicate new acceptor does not complete the second quorum",
            !processor.getAccepted());
        promise(processor, 6);
        MembershipChecks.check("three promises in each group complete recovery", processor.getAccepted());
      }
    }
    var processor = new PhaseOneResponseProcessor(schedule, 0, 3);
    for (int id : new int[] {4, 5, 6}) promise(processor, id);
    MembershipChecks.check("new quorum alone cannot finish recovery", !processor.getAccepted());
    processor = new PhaseOneResponseProcessor(schedule, 0, 3);
    for (int id : new int[] {2, 3, 4}) promise(processor, id);
    MembershipChecks.check("shared acceptors can form both quorums", processor.getAccepted());
    processor = new PhaseOneResponseProcessor(schedule, 0, 2);
    for (int id : new int[] {0, 1, 4}) promise(processor, id);
    MembershipChecks.check("first transfer only needs the old group's prepare quorum", processor.getAccepted());
    processor = new PhaseOneResponseProcessor(schedule, 2, 3);
    for (int id : new int[] {4, 5, 6}) promise(processor, id);
    MembershipChecks.check("completed new reference does not require removed acceptors", processor.getAccepted());
  }

  static int decision(DidaTradeServerState state) {
    synchronized (state) {
      var entry = state.paxos_log.getEntry(0);
      return entry != null && entry.decided ? entry.command_id : -1;
    }
  }

  static void run(int port) throws Exception {
    quorumChecks();
    try (var c = new MembershipChecks.Cluster(port)) {
      // Fast ballot 0: two votes for X, two for Y. Neither has a supermajority.
      for (int i = 0; i < 7; i++) {
        MembershipChecks.pending(c.states[i], 101, MembershipChecks.sell(0));
        MembershipChecks.pending(c.states[i], 201, MembershipChecks.sell(1));
        if (i < 4) {
          var entry = c.states[i].paxos_log.testAndSetEntry(0);
          entry.accepted_value = i < 2 ? 101 : 201;
          entry.write_ballot = 0;
          entry.accepted_fast = true;
        }
        final int id = i;
        c.servers[i] = ServerBuilder.forPort(port + i)
            .addService(new DidaTradeMasterServiceImpl(c.states[i]))
            .addService(new DidaTradePaxosServiceImpl(c.states[i]) {
              @Override public void phaseone(DidaTradePaxos.PhaseOneRequest request,
                  StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
                // Force the old group's ballot-3 replies to be S0, S1 and S4.
                if (request.getRequestballot() == 3 && (id == 2 || id == 3))
                  observer.onError(Status.UNAVAILABLE.asRuntimeException());
                else super.phaseone(request, observer);
              }

              @Override public void learn(DidaTradePaxos.LearnRequest request,
                  StreamObserver<DidaTradePaxos.LearnReply> observer) {
                // Only S5 learns ballot 2's decision; the other notifications are lost.
                if (request.getBallot() == 2 && id != 5) {
                  observer.onNext(DidaTradePaxos.LearnReply.getDefaultInstance());
                  observer.onCompleted();
                } else super.learn(request, observer);
              }
            }).build().start();
      }
      var processor = new PhaseOneResponseProcessor(c.states[0].scheduler, 0, 2);
      var replies = new ArrayList<DidaTradePaxos.PhaseOneReply>();
      for (int id : new int[] {2, 0, 4}) {
        var reply = DidaTradePaxosServiceGrpc.newBlockingStub(c.states[0].channels[id])
            .withDeadlineAfter(5, TimeUnit.SECONDS).phaseone(
                DidaTradePaxos.PhaseOneRequest.newBuilder().setRequestballot(2).build());
        replies.add(reply);
        processor.onNext(replies, reply);
      }
      int recovered = processor.getAcceptedInstances().get(0).getValue();
      MembershipChecks.check("ballot 2 can recover Y from Y, X and an empty reply",
          processor.getAccepted() && recovered == 201);
      for (int id : new int[] {3, 5, 6}) {
        var reply = DidaTradePaxosServiceGrpc.newBlockingStub(c.states[0].channels[id])
            .withDeadlineAfter(5, TimeUnit.SECONDS).phasetwo(
                DidaTradePaxos.PhaseTwoRequest.newBuilder().setRequestballot(2)
                    .setValue(recovered).build());
        MembershipChecks.check("ballot 2 transfer accepted on S" + id, reply.getAccepted());
      }
      MembershipChecks.await("S5 decides Y before ballot 2 reports completion",
          () -> decision(c.states[5]) == 201);

      // Run the real leader loop: checking the processor alone would not check its RPC targets.
      var leader = c.states[4];
      leader.main_loop_worker = new Thread(leader.main_loop);
      leader.main_loop_worker.start();
      var completed = c.master(4).newballot(DidaTradeMaster.NewBallotRequest.newBuilder()
          .setNewballot(3).setCompletedballot(0).build());
      MembershipChecks.check("ballot 3 completes the recovery", completed.getCompletedballot() == 3);
      MembershipChecks.await("S4 and S6 preserve the same Y already decided by S5",
          () -> decision(c.states[4]) == 201 && decision(c.states[6]) == 201);
      for (int i = 0; i < 7; i++)
        MembershipChecks.check("S" + i + " did not decide X", decision(c.states[i]) != 101);
    }
  }
}
