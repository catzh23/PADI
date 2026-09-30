package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import java.util.*;

public class MainLoop implements Runnable {
  // Limit how far consensus may run ahead of ordered execution.
  static final int PIPELINE_WINDOW = 16;
  private static final long RETRY_NANOS = 250_000_000L;
  private final DidaTradeServerState server_state;
  private boolean has_work;
  private int next_log_entry;
  private int active_ballot = -1;
  private int prepared_ballot = -1;
  private long prepare_retry_after;
  private PhaseOneProcessor prepare_processor;
  private GenericResponseCollector<DidaTradePaxos.PhaseOneReply> prepare_collector;
  // Keep a proposal's value even if its RPCs fail: never change it within a ballot.
  private final TreeMap<Integer, Integer> prepared_values = new TreeMap<>();
  private final Map<Integer, Proposal> in_flight = new HashMap<>();
  private final Map<Integer, Long> retry_after = new HashMap<>();

  private record Proposal(
      int value,
      PhaseTwoResponseProcessor processor,
      GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector) {}

  public MainLoop(DidaTradeServerState state) {
    this.server_state = state;
  }

  public synchronized void wakeup() {
    this.has_work = true;
    notifyAll();
  }

  // RPC callbacks collect responses and wake this loop; only this thread executes commands.
  @Override
  public synchronized void run() {
    while (!Thread.currentThread().isInterrupted()) {
      this.has_work = false;
      int ballot = this.server_state.getCurrentBallot();
      if (ballot != this.active_ballot) {
        this.active_ballot = ballot;
        this.prepared_ballot = -1;
        this.prepare_collector = null;
        this.prepare_processor = null;
        this.prepare_retry_after = 0;
        this.prepared_values.clear();
        this.in_flight.clear();
        this.retry_after.clear();
        this.server_state.setLastAcceptedBallot(-1);
      }

      finishPrepare(ballot);
      finishProposals(ballot);
      executeReadyEntries();
      if (ballot != this.server_state.getCurrentBallot()) continue;

      if (ballot >= 0 && this.server_state.scheduler.leader(ballot) == this.server_state.my_id) {
        if (this.prepared_ballot == ballot) {
          proposeAvailable(ballot);
        } else if (this.prepare_collector == null
            && System.nanoTime() >= this.prepare_retry_after
            && (ballot > 0 || this.server_state.req_history.getFirstPending() != null)) {
          startPrepare(ballot);
        }
      }
      if (!this.has_work) {
        try {
          // Also retry failed RPC rounds and observe promises advanced by inbound RPCs.
          wait(250);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private <T> CollectorStreamObserver<T> observer(GenericResponseCollector<T> collector) {
    return new CollectorStreamObserver<T>(collector) {
      @Override
      public void onNext(T response) {
        super.onNext(response);
        wakeup();
      }

      @Override
      public void onError(Throwable error) {
        super.onError(error);
        wakeup();
      }

      @Override
      public void onCompleted() {
        super.onCompleted();
        wakeup();
      }
    };
  }

  private void startPrepare(int ballot) {
    List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
    this.prepare_processor = PhaseOneProcessor.create(
        this.server_state.scheduler, Math.max(this.server_state.getCompletedBallot(), 0), ballot);
    this.prepare_collector = new GenericResponseCollector<>(
        new ArrayList<>(), acceptors.size(), this.prepare_processor);
    var request = DidaTradePaxos.PhaseOneRequest.newBuilder()
        .setInstance(this.next_log_entry).setRequestballot(ballot).build();
    System.out.println("Going to run paxos phase 1 for ballot " + ballot);
    for (int acceptor : acceptors)
      this.server_state.async_stubs[acceptor].phaseone(request, observer(this.prepare_collector));
  }

  private void finishPrepare(int ballot) {
    if (this.prepare_collector == null || !this.prepare_collector.isDone()) return;
    boolean accepted = this.prepare_processor.getAccepted();
    synchronized (this.server_state) {
      if (!accepted) this.server_state.setCurrentBallot(this.prepare_processor.getMaxballot());
      if (accepted && ballot == this.server_state.getCurrentBallot()) {
        this.prepared_values.clear();
        for (var entry : this.prepare_processor.getAcceptedInstances().values()) {
          if (entry.getInstance() >= this.next_log_entry)
            this.prepared_values.put(entry.getInstance(), entry.getValue());
        }
        this.prepared_ballot = ballot;
        this.server_state.setLastAcceptedBallot(ballot);
        System.out.println("Multi-prepare completed for ballot " + ballot
            + "; recovered slots = " + this.prepared_values.keySet());
      }
    }
    this.prepare_collector = null;
    this.prepare_processor = null;
    this.prepare_retry_after = System.nanoTime() + RETRY_NANOS;
  }

  private void finishProposals(int ballot) {
    var iterator = this.in_flight.entrySet().iterator();
    while (iterator.hasNext()) {
      var item = iterator.next();
      int slot = item.getKey();
      Proposal proposal = item.getValue();
      if (!proposal.collector().isDone()) continue;
      iterator.remove();
      if (proposal.processor().getAccepted()) {
        synchronized (this.server_state) {
          // A ballot change abandons its rounds. Their late callbacks cannot decide new slots.
          if (ballot != this.server_state.getCurrentBallot()) continue;
          PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(slot);
          if (!entry.decided) {
            entry.command_id = proposal.value();
            entry.accept_ballot = ballot;
            entry.decided = true;
            this.server_state.updateCompletedBallot(ballot);
            System.out.println("Phase 2 chose slot " + slot + " in ballot " + ballot);
          }
        }
      } else {
        this.server_state.setCurrentBallot(proposal.processor().getMaxballot());
        this.retry_after.put(slot, System.nanoTime() + RETRY_NANOS);
      }
    }
  }

  private void proposeAvailable(int ballot) {
    Set<Integer> reserved = new HashSet<>(this.prepared_values.values());
    // A learned but not executed request must not be proposed again for a hole.
    for (int slot = this.next_log_entry; slot < this.next_log_entry + PIPELINE_WINDOW; slot++) {
      PaxosInstance entry = this.server_state.paxos_log.getEntry(slot);
      if (entry != null && entry.decided) reserved.add(entry.command_id);
    }
    for (int slot = this.next_log_entry; slot < this.next_log_entry + PIPELINE_WINDOW; slot++) {
      if (ballot != this.server_state.getCurrentBallot()) return;
      PaxosInstance entry = this.server_state.paxos_log.getEntry(slot);
      if ((entry != null && entry.decided) || this.in_flight.containsKey(slot)) continue;
      if (System.nanoTime() < this.retry_after.getOrDefault(slot, 0L)) continue;
      Integer value = this.prepared_values.get(slot);
      if (value == null) {
        RequestRecord pending = this.server_state.req_history.getFirstPending(reserved);
        if (pending != null) value = pending.getId();
        else if (this.prepared_values.higherKey(slot) != null) value = PaxosInstance.NO_OP;
        else break;
      }
      this.prepared_values.put(slot, value);
      reserved.add(value);
      this.retry_after.remove(slot);
      startProposal(slot, value, ballot);
    }
  }

  private void startProposal(int slot, int value, int ballot) {
    List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
    var processor = new PhaseTwoResponseProcessor(this.server_state.scheduler.quorum(ballot));
    var collector = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(
        new ArrayList<>(), acceptors.size(), processor);
    this.in_flight.put(slot, new Proposal(value, processor, collector));
    var request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
        .setInstance(slot).setValue(value).setRequestballot(ballot).build();
    System.out.println("Sending phase 2 for slot " + slot + " in ballot " + ballot + ", value " + value);
    for (int acceptor : acceptors)
      this.server_state.async_stubs[acceptor].phasetwo(request, observer(collector));
  }

  private void executeReadyEntries() {
    while (true) {
      PaxosInstance entry = this.server_state.paxos_log.getEntry(this.next_log_entry);
      if (entry == null || !entry.decided) return;
      if (entry.command_id != PaxosInstance.NO_OP
          && this.server_state.req_history.getIfProcessed(entry.command_id) == null) {
        RequestRecord request = this.server_state.req_history.getIfPending(entry.command_id);
        if (request == null) return;
        execute(entry, request);
      }
      System.out.println("Log entry with number " + this.next_log_entry
          + " has been decided with command id = " + entry.command_id);
      this.prepared_values.remove(this.next_log_entry);
      this.in_flight.remove(this.next_log_entry);
      this.retry_after.remove(this.next_log_entry);
      this.next_log_entry++;
    }
  }

  private void execute(PaxosInstance next_entry, RequestRecord request_record) {
    DidaTradeCommand command = request_record.getRequest();
    boolean result = false;
    int balance = 0;

    DidaTradeAction action = command.getAction();

    // System.out.println("Action  = " + action);
    switch (action) {
      case DidaTradeAction.POPULATE:
        result = this.server_state.trade_manager.populate(command.getQuantity());
        break;
      case DidaTradeAction.ADDUSER:
        result =
            this.server_state.trade_manager.add_user(
                command.getUserId(), command.getQuantity(), command.getStock());
        break;
      case DidaTradeAction.SELL:
        result = this.server_state.trade_manager.sell(command.getUserId(), command.getQuantity());
        break;
      case DidaTradeAction.BUY:
        result =
            this.server_state.trade_manager.acquire(command.getUserId(), command.getQuantity());
        break;
      case DidaTradeAction.BALANCE:
        balance = this.server_state.trade_manager.balance(command.getUserId());
        command.setQuantity(balance);
        result = (balance != -1);
        break;
      case DidaTradeAction.DUMP:
        this.server_state.trade_manager.dump();
        result = true;
        break;
      default:
        result = false;
        System.err.println("*** Unknown command ****");
        break;
    }

    // sending response
    System.out.println(
        "Setting response for command with id = "
            + next_entry.command_id
            + " with result = "
            + result);
    request_record.setResponse(result);
    this.server_state.req_history.moveToProcessed(request_record.getId());
  }
}
