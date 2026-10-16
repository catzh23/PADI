package didatrade.server;

import didatrade.DebugInterceptor;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.configs.*;
import didatrade.configs.ConfigurationScheduler;
import didatrade.core.*;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.*;

public class DidaTradeServerState {
  public static final int DEFAULT_POPULATION = 10;
  int max_participants;
  TradeManager trade_manager;
  ConfigurationScheduler scheduler;
  int base_port;
  int my_id;
  RequestHistory req_history;
  PaxosLog paxos_log;

  List<Integer> all_participants;
  int n_participants;
  String[] targets;
  ManagedChannel[] channels;
  DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;
  DebugInterceptor debug_interceptor;

  private int current_ballot;
  private int completed_ballot;
  private int preparation_ballot = -1;
  private int reference_ballot = -1;
  private int ready_ballot = -1;
  private int activated_ballot = -1;
  private int automatic_ballot = -1;
  /* count of log entries this replica has executed */
  private int executed_index;

  private int debug_mode;
  private boolean fastpaxos_on;
  private int any_ballot; // the ballot this acceptor received 'any' for
  private int any_from; // the first slot this acceptor received 'any' for

  MainLoop main_loop;
  Thread main_loop_worker;

  public DidaTradeServerState(int port, int myself, char schedule) {
    this.trade_manager = new TradeManager();
    this.scheduler = new ConfigurationScheduler(schedule);
    this.base_port = port;
    this.my_id = myself;
    this.debug_interceptor = new DebugInterceptor();
    this.debug_mode = 0;
    this.fastpaxos_on = false;
    this.any_ballot = -1;
    this.current_ballot = 0;
    this.completed_ballot = -1;
    this.executed_index = 0;
    this.req_history = new RequestHistory();
    this.paxos_log = new PaxosLog();
    this.main_loop = new MainLoop(this);

    // populate manager
    this.trade_manager.populate(DEFAULT_POPULATION);

    // init comms
    this.all_participants = this.scheduler.allparticipants();
    this.n_participants = all_participants.size();

    this.targets = new String[this.n_participants];
    for (int i = 0; i < this.n_participants; i++) {
      int target_port = this.base_port + all_participants.get(i);
      this.targets[i] = new String();
      this.targets[i] = "localhost:" + target_port;
      System.out.printf("targets[%d] = %s%n", i, targets[i]);
    }

    this.channels = new ManagedChannel[this.n_participants];
    for (int i = 0; i < this.n_participants; i++)
      this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();

    this.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[this.n_participants];
    for (int i = 0; i < this.n_participants; i++)
      this.async_stubs[i] = DidaTradePaxosServiceGrpc.newStub(this.channels[i]);

    // start worker
    this.main_loop_worker = new Thread(main_loop);
    this.main_loop_worker.start();
  }

  public synchronized int getAnyBallot() {
    return this.any_ballot;
  }

  public synchronized void setAnyBallot(int ballot) {
    this.any_ballot = ballot;
  }

  public synchronized int getAnyFrom() {
    return this.any_from;
  }

  public synchronized void setAnyFrom(int from) {
    this.any_from = from;
  }

  public synchronized int getCurrentBallot() {
    return this.current_ballot;
  }

  public synchronized void setCurrentBallot(int ballot) {
    if (this.scheduler.validBallot(ballot) && ballot > this.current_ballot) {
      this.current_ballot = ballot;
      notifyAll();
    }
  }

  public synchronized boolean beginPreparation(int ballot, int reference) {
    if (!this.scheduler.validBallot(ballot)
        || ballot < this.current_ballot
        || this.scheduler.leader(ballot) != this.my_id
        || reference < 0
        || reference >= ballot) return false;
    if (this.preparation_ballot == ballot) return this.reference_ballot == reference;
    this.preparation_ballot = ballot;
    this.reference_ballot = reference;
    setCurrentBallot(ballot);
    return true;
  }

  public synchronized int getReferenceBallot(int ballot) {
    return this.preparation_ballot == ballot ? this.reference_ballot : -1;
  }

  public synchronized void advanceAfterCollision(int ballot) {
    int next = ballot + 1;
    if (ballot != this.current_ballot || !this.scheduler.validBallot(next)) return;
    setCurrentBallot(next);
    // Fast votes in ballot 2 mean its transfer was completed and activated.
    // Its successor uses the same acceptors, so no new membership needs approval.
    if (this.scheduler.needsActivation(next)
        && this.scheduler.fastpaxos(ballot)
        && this.scheduler.acceptors(ballot).equals(this.scheduler.acceptors(next))
        && beginPreparation(next, ballot)) {
      this.automatic_ballot = next;
    }
  }

  public synchronized void finishPreparation(int ballot) {
    if (ballot != this.current_ballot || ballot != this.preparation_ballot) return;
    if (this.ready_ballot == ballot) return;
    this.ready_ballot = ballot;
    this.completed_ballot = ballot;
    if (this.automatic_ballot == ballot) {
      activate(ballot);
      System.out.println("Automatic recovery complete for ballot " + ballot);
    } else {
      System.out.println("Preparation complete for ballot " + ballot + "; waiting for activate");
    }
    notifyAll();
  }

  public synchronized boolean waitForPreparation(int ballot) {
    while (this.current_ballot == ballot && this.ready_ballot != ballot) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return this.current_ballot == ballot && this.ready_ballot == ballot;
  }

  public synchronized boolean activate(int ballot) {
    if (ballot != this.current_ballot || ballot != this.ready_ballot) return false;
    this.activated_ballot = ballot;
    System.out.println("Activated ballot " + ballot);
    return true;
  }

  public synchronized boolean canProposeNewRequests(int ballot) {
    return !this.scheduler.needsActivation(ballot) || this.activated_ballot == ballot;
  }

  public synchronized int getCompletedBallot() {
    return this.completed_ballot;
  }

  public int findMaxDecidedBallot() {
    int ballot = -1;
    int length = this.paxos_log.length();

    for (int i = 0; i < length; i++) {
      PaxosInstance entry = this.paxos_log.getEntry(i);
      if (entry == null) return ballot;
      if (!entry.decided) return ballot;
      else if (entry.accept_ballot > ballot) ballot = entry.accept_ballot;
    }
    return ballot;
  }

  public synchronized void updateCompletedBallot(int ballot) {
    // In Schedule C a decided slot does not certify a completed state transfer.
    if (this.scheduler.needsActivation(this.current_ballot)) return;
    // Fixed membership keeps the baseline completion rule. Transfers use finishPreparation.

    ballot = this.findMaxDecidedBallot();
    if (ballot > this.completed_ballot) this.completed_ballot = ballot;
    this.notifyAll();
  }

  public synchronized void setCompletedBallot(int ballot) {
    if (ballot > this.completed_ballot) this.completed_ballot = ballot;
    this.notifyAll();
  }

  public synchronized int waitForCompletedBallot(int ballot) {
    while (this.completed_ballot < ballot) {
      try {
        wait();
      } catch (InterruptedException e) {
      }
    }
    return this.completed_ballot;
  }

  public synchronized boolean getFastPaxosMode() {
    return this.fastpaxos_on;
  }

  public synchronized void setFastPaxosMode(boolean mode) {
    this.fastpaxos_on = mode;
  }

  public synchronized int getDebugMode() {
    return this.debug_mode;
  }

  public synchronized void setDebugMode(int mode) {
    this.debug_mode = mode;
  }

  // Helper Function so that it's available to both MainLoop and DidaTradePaxosServiceImpl
  public void sendLearn(int instance, int ballot, int value, boolean fast) {
    Context ctx = Context.current().fork();
    ctx.run(
        () -> {
          List<Integer> learners = this.scheduler.learners(ballot);
          int n_targets = learners.size();

          DidaTradePaxos.LearnRequest.Builder learn_request_builder =
              DidaTradePaxos.LearnRequest.newBuilder();
          learn_request_builder.setInstance(instance);
          learn_request_builder.setValue(value);
          learn_request_builder.setBallot(ballot);
          learn_request_builder.setServerid(this.my_id);
          learn_request_builder.setFast(fast);
          RequestRecord record = this.req_history.getIfExists(value);
          if (record != null && this.scheduler.needsActivation(ballot))
            learn_request_builder.setCommand(record.getRequest().toMessage());

          DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();

          // System.out.println("Sending learn request: \n" + learn_request);

          System.out.println(
              "Paxos acceptor: going to notify learners for entry "
                  + instance
                  + " with timestamp "
                  + ballot
                  + " request = "
                  + learn_request);
          ArrayList<DidaTradePaxos.LearnReply> learn_responses =
              new ArrayList<DidaTradePaxos.LearnReply>();
          GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector =
              new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);
          ;
          for (int i = 0; i < n_targets; i++) {
            CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer =
                new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
            this.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
          }
          // System.out.println("Learn request completed for instance = " + instance);
        });
  }

  public synchronized int getExecutedIndex() {
    return this.executed_index;
  }

  public synchronized void setExecutedIndex(int index) {
    if (index > this.executed_index) this.executed_index = index;
    this.notifyAll();
  }

  public synchronized void waitForExecutedIndex(int index) {
    while (this.executed_index < index) {
      try {
        wait();
      } catch (InterruptedException e) {
      }
    }
  }
}
