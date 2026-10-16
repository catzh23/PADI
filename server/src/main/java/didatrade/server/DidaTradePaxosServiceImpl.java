package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.*;

public class DidaTradePaxosServiceImpl
    extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
  DidaTradeServerState server_state;

  public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
    this.server_state = state;
  }

  @Override
  public void phaseone(
      DidaTradePaxos.PhaseOneRequest request,
      StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
    // System.out.println("Receive phase1 request: \n" + request);

    int instance = request.getInstance();
    int ballot = request.getRequestballot();
    DidaTradePaxos.PhaseOneReply response;
    synchronized (this.server_state) {
      PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance, ballot);
      boolean accepted = false;
      int value = entry.accepted_value;
      int valballot = entry.write_ballot;

      if (this.server_state.scheduler.validBallot(ballot)
          && ballot >= this.server_state.getCurrentBallot()) {
        accepted = true;
        this.server_state.setCurrentBallot(ballot);
        entry.read_ballot = ballot;
      }

      int maxballot = this.server_state.getCurrentBallot();

      // System.out.println("Instance = " + instance + " ballot = " + ballot + " current_ballot = "
      // +
      // this.server_state.getCurrentBallot() + " val = " + value + " valballot = " + valballot + "
      // maxballot = " + maxballot + " accepted = " + accepted);

      DidaTradePaxos.PhaseOneReply.Builder response_builder =
          DidaTradePaxos.PhaseOneReply.newBuilder();
      response_builder.setInstance(instance);
      response_builder.setServerid(this.server_state.my_id);
      response_builder.setRequestballot(ballot);
      response_builder.setAccepted(accepted);
      response_builder.setValue(value);
      response_builder.setValballot(valballot);
      response_builder.setMaxballot(maxballot);

      if (accepted) {
        for (var saved : this.server_state.paxos_log.acceptedInstances(instance)) {
          var builder = saved.toBuilder();
          RequestRecord record = this.server_state.req_history.getIfExists(saved.getValue());
          if (record != null) builder.setCommand(record.getRequest().toMessage());
          response_builder.addAcceptedinstances(builder);
        }
      }
      response = response_builder.build();
    }

    // Wake outside the state lock: the loop acquires that lock while running.
    this.server_state.main_loop.wakeup();

    // System.out.println("Sending phase1 response: " + response);

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void phasetwo(
      DidaTradePaxos.PhaseTwoRequest request,
      StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
    // System.out.println ("Receive phase two request: \n" + request);

    int instance = request.getInstance();
    int ballot = request.getRequestballot();
    int value = request.getValue();
    boolean accepted = false;
    int maxballot = ballot;

    synchronized (this.server_state) {
      PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
      if (this.server_state.scheduler.validBallot(ballot)
          && this.server_state.scheduler.isacceptor(this.server_state.my_id, ballot)
          && ballot >= this.server_state.getCurrentBallot()) {
        accepted = true;
        if (request.hasCommand())
          this.server_state.req_history.rememberTransferred(
              value, DidaTradeCommand.fromMessage(request.getCommand()));
        entry.accepted_value = value;
        entry.write_ballot = ballot;
        entry.accepted_fast = false;
        this.server_state.setCurrentBallot(ballot);
      } else maxballot = this.server_state.getCurrentBallot();
    }

    this.server_state.main_loop.wakeup();

    DidaTradePaxos.PhaseTwoReply.Builder response_builder =
        DidaTradePaxos.PhaseTwoReply.newBuilder();
    response_builder.setAccepted(accepted);
    response_builder.setInstance(instance);
    response_builder.setServerid(this.server_state.my_id);
    response_builder.setRequestballot(ballot);
    response_builder.setMaxballot(maxballot);

    DidaTradePaxos.PhaseTwoReply response = response_builder.build();

    // System.out.println("Sending phase2 response: " + response);

    responseObserver.onNext(response);
    responseObserver.onCompleted();

    // Notify learners
    if (accepted == true) this.server_state.sendLearn(instance, ballot, value, false);
  }

  @Override
  public void any(
      DidaTradePaxos.AnyRequest request, StreamObserver<DidaTradePaxos.AnyReply> responseObserver) {
    int instance = request.getInstance();
    int ballot = request.getRequestballot();
    boolean accepted = false;
    int maxballot = ballot;

    synchronized (this.server_state) {
      if (this.server_state.scheduler.validBallot(ballot)
          && this.server_state.scheduler.isacceptor(this.server_state.my_id, ballot)
          && ballot >= this.server_state.getCurrentBallot()) {
        accepted = true;
        this.server_state.setAnyBallot(ballot);
        this.server_state.setAnyFrom(instance);
        this.server_state.setCurrentBallot(ballot);
      } else maxballot = this.server_state.getCurrentBallot();
    }

    DidaTradePaxos.AnyReply.Builder response_builder = DidaTradePaxos.AnyReply.newBuilder();
    response_builder.setServerid(this.server_state.my_id);
    response_builder.setRequestballot(ballot);
    response_builder.setAccepted(accepted);
    response_builder.setMaxballot(maxballot);

    DidaTradePaxos.AnyReply response = response_builder.build();

    // System.out.println("Sending any response: " + response);

    responseObserver.onNext(response);
    responseObserver.onCompleted();

    this.server_state.main_loop.wakeup();
  }

  @Override
  public void learn(
      DidaTradePaxos.LearnRequest request,
      StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
    // System.out.println("Receive learn request: \n" + request);

    int instance = request.getInstance();
    int ballot = request.getBallot();
    int value = request.getValue();

    boolean wakeup = false;
    synchronized (this.server_state) {
      PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
      boolean member =
          this.server_state.scheduler.validBallot(ballot)
              && this.server_state.scheduler.acceptors(ballot).contains(request.getServerid());
      if (member) {
        wakeup = ballot > this.server_state.getCurrentBallot();
        this.server_state.setCurrentBallot(ballot);
        if (request.hasCommand()) {
          this.server_state.req_history.rememberTransferred(
              value, DidaTradeCommand.fromMessage(request.getCommand()));
          wakeup = true;
        }
      }
      if (member && !entry.decided) {
        if (ballot > entry.accept_ballot) {
          entry.accept_ballot = ballot;
          entry.learning_fast = false;
          entry.learning_votes.clear();
        }

        if (ballot == entry.accept_ballot) {
          entry
              .learning_votes
              .computeIfAbsent(value, k -> new HashSet<>())
              .add(request.getServerid());
          if (request.getFast()) entry.learning_fast = true;

          int needed_quorum =
              entry.learning_fast
                  ? this.server_state.scheduler.supermajority(ballot)
                  : this.server_state.scheduler.quorum(ballot);
          if (entry.learning_votes.get(value).size() >= needed_quorum) {
            entry.command_id = value;
            entry.decided = true;
            System.out.println(
                "Paxos learner decided slot "
                    + instance
                    + " in ballot "
                    + ballot
                    + ", value "
                    + entry.command_id);
            this.server_state.updateCompletedBallot(ballot);
            wakeup = true;
          } else if (entry.learning_fast) {
            int max_votes = 0;
            int total_votes = 0;
            for (Set<Integer> votes : entry.learning_votes.values()) {
              if (votes.size() > max_votes) max_votes = votes.size();
              total_votes += votes.size();
            }
            int remaining = this.server_state.scheduler.acceptors(ballot).size() - total_votes;

            if (max_votes + remaining < needed_quorum) {
              System.out.println(
                  "Collision detected for slot " + instance + " in ballot " + ballot);
              if (ballot == this.server_state.getCurrentBallot()
                  && this.server_state.scheduler.validBallot(ballot + 1)) {
                this.server_state.advanceAfterCollision(ballot);
                wakeup = true;
              }
            }
          }
        }
      }
    }
    // MainLoop also acquires the state lock: never wake it while holding that lock.
    if (wakeup) this.server_state.main_loop.wakeup();
    DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
    response_builder.setInstance(instance);
    response_builder.setBallot(ballot);

    DidaTradePaxos.LearnReply response = response_builder.build();

    // System.out.println("Sending learn response");

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
