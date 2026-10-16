package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneProcessor;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MainLoop implements Runnable {
  // Limit how far consensus may run ahead of ordered execution.
  static final int PIPELINE_WINDOW = 16;
  private static final long RETRY_AFTER_FAIL_TIME = 250; // milliseconds
  private ScheduledExecutorService retry_timer;
  private final DidaTradeServerState server_state;
  private boolean has_work;
  private int next_log_entry;
  private int active_ballot = -1;
  private int prepared_ballot = -1;
  private boolean fast_active = false; // whether running fast paxos
  private int any_from = -1; // first slot handed over to acceptors with 'any'
  private boolean any_sent = false; // tells whether 'any' was sent
  private long any_retry_after = 0; // for when 'any' message doesn't reach all replicas
  private long prepare_retry_after;
  private PhaseOneProcessor prepare_processor;
  private GenericResponseCollector<DidaTradePaxos.PhaseOneReply> prepare_collector;
  private GenericResponseCollector<DidaTradePaxos.AnyReply> any_collector;
  // Keep a proposal's value even if its RPCs fail: never change it within a ballot.
  private final TreeMap<Integer, Integer> prepared_values = new TreeMap<>();
  // These slots must reach the new acceptors, even if this leader already executed them.
  private final TreeMap<Integer, Integer> transfer_remaining = new TreeMap<>();
  private final Map<Integer, Proposal> in_flight = new HashMap<>();
  private final Map<Integer, Long> retry_after = new HashMap<>();
  // request -> slot where this acceptor voted for it in the current ballot
  private final Map<Integer, Integer> fast_placed = new HashMap<>();

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
    this.retry_timer =
        Executors.newSingleThreadScheduledExecutor(
            task -> {
              Thread timer = new Thread(task, "paxos-retry");
              timer.setDaemon(true);
              return timer;
            });
    try {
      while (!Thread.currentThread().isInterrupted()) {
        this.has_work = false;
        int ballot = this.server_state.getCurrentBallot();
        if (ballot != this.active_ballot) {
          this.active_ballot = ballot;
          this.prepared_ballot = -1;
          this.fast_active = false;
          this.any_from = -1;
          this.any_sent = false;
          this.any_retry_after = 0;
          this.prepare_collector = null;
          this.any_collector = null;
          this.prepare_processor = null;
          this.prepare_retry_after = 0;
          this.prepared_values.clear();
          this.transfer_remaining.clear();
          this.in_flight.clear();
          this.retry_after.clear();
          this.fast_placed.clear();
        }

        finishAny(ballot);
        finishPrepare(ballot);
        finishProposals(ballot);
        executeReadyEntries();
        if (ballot != this.server_state.getCurrentBallot()) continue;

        fastAccept(ballot);

        if (ballot >= 0 && this.server_state.scheduler.leader(ballot) == this.server_state.my_id) {
          if (this.prepared_ballot == ballot) {
            if (!this.server_state.canProposeNewRequests(ballot)) {
              transferHistory(ballot);
            } else {
              if (!any_sent && this.fast_active && System.nanoTime() >= this.any_retry_after)
                startAny(ballot);
              proposeAvailable(ballot);
            }
          } else if (this.prepare_collector == null
              && System.nanoTime() >= this.prepare_retry_after
              && (ballot > 0
                  || this.server_state.req_history.getFirstPending() != null
                  || this.server_state.scheduler.fastpaxos(ballot))) {
            startPrepare(ballot);
          }
        }
        if (!this.has_work) {
          try {
            wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    } finally {
      this.retry_timer.shutdownNow();
    }
  }

  private long retryLater() {
    // Wake when the retry is due, even if no new request or RPC response arrives.
    long retry_at = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RETRY_AFTER_FAIL_TIME);
    this.retry_timer.schedule(this::wakeup, RETRY_AFTER_FAIL_TIME, TimeUnit.MILLISECONDS);
    return retry_at;
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
    boolean transfer = this.server_state.scheduler.needsActivation(ballot);
    int reference = transfer ? this.server_state.getReferenceBallot(ballot) : ballot;
    // A learner can hear about the new ballot before the console supplies its reference.
    if (reference < 0) return;
    List<Integer> acceptors = this.server_state.scheduler.acceptors(reference);
    if (transfer) {
      var processor = new PhaseOneResponseProcessor(this.server_state.scheduler, reference, ballot);
      this.prepare_processor = processor;
      acceptors = processor.getAcceptors();
    } else {
      this.prepare_processor =
          PhaseOneProcessor.create(
              this.server_state.scheduler,
              Math.max(this.server_state.getCompletedBallot(), 0),
              ballot);
    }
    this.prepare_collector =
        new GenericResponseCollector<>(new ArrayList<>(), acceptors.size(), this.prepare_processor);
    var request =
        DidaTradePaxos.PhaseOneRequest.newBuilder()
            .setInstance(transfer ? 0 : this.next_log_entry)
            .setRequestballot(ballot)
            .build();
    System.out.println("Going to run paxos phase 1 for ballot " + ballot);
    if (transfer)
      System.out.println("Recovering ballot " + ballot + " from configuration " + reference);
    for (int acceptor : acceptors)
      this.server_state.async_stubs[acceptor].phaseone(request, observer(this.prepare_collector));
  }

  private void finishPrepare(int ballot) {
    if (this.prepare_collector == null || !this.prepare_collector.isDone()) return;
    boolean accepted = this.prepare_processor.getAccepted();
    synchronized (this.server_state) {
      if (!accepted) this.server_state.setCurrentBallot(this.prepare_processor.getMaxballot());
      if (accepted && ballot == this.server_state.getCurrentBallot()) {
        boolean transfer = this.server_state.scheduler.needsActivation(ballot);
        var recovered = this.prepare_processor.getAcceptedInstances();
        // A request ID alone cannot restore the application on a replica that missed it.
        for (var entry : recovered.values()) {
          if (entry.hasCommand())
            this.server_state.req_history.rememberTransferred(
                entry.getValue(), DidaTradeCommand.fromMessage(entry.getCommand()));
          if (transfer
              && entry.getValue() != PaxosInstance.NO_OP
              && this.server_state.req_history.getIfExists(entry.getValue()) == null) {
            this.prepare_collector = null;
            this.prepare_processor = null;
            this.prepare_retry_after = retryLater();
            return;
          }
        }
        this.prepared_values.clear();
        this.transfer_remaining.clear();
        int last_slot = -1;
        for (var entry : recovered.values()) {
          last_slot = Math.max(last_slot, entry.getInstance());
          if (transfer) this.transfer_remaining.put(entry.getInstance(), entry.getValue());
          else if (entry.getInstance() >= this.next_log_entry)
            this.prepared_values.put(entry.getInstance(), entry.getValue());
        }
        if (transfer) {
          // Empty holes in the recovered prefix cannot be filled with client requests
          // before activation. No-ops allow lagging learners to execute that prefix.
          for (int slot = 0; slot <= last_slot; slot++)
            this.transfer_remaining.putIfAbsent(slot, PaxosInstance.NO_OP);
        }
        this.prepared_ballot = ballot;
        fast_active = this.server_state.scheduler.fastpaxos(ballot);
        any_from = Math.max(this.next_log_entry, last_slot + 1);
        System.out.println(
            "Multi-prepare completed for ballot "
                + ballot
                + "; recovered slots = "
                + (transfer ? this.transfer_remaining.keySet() : this.prepared_values.keySet()));
      }
    }
    this.prepare_collector = null;
    this.prepare_processor = null;
    if (!accepted) this.prepare_retry_after = retryLater();
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
          if (this.transfer_remaining.remove(slot) != null)
            System.out.println("Transferred slot " + slot + " in ballot " + ballot);
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
        this.retry_after.put(slot, retryLater());
      }
    }
  }

  private void transferHistory(int ballot) {
    if (this.transfer_remaining.isEmpty()) {
      this.server_state.finishPreparation(ballot);
      // Automatic recovery may activate here, without a later console RPC.
      if (this.server_state.canProposeNewRequests(ballot)) wakeup();
      return;
    }
    for (var item : this.transfer_remaining.entrySet()) {
      if (this.in_flight.size() >= PIPELINE_WINDOW) break;
      int slot = item.getKey();
      if (this.in_flight.containsKey(slot)) continue;
      if (System.nanoTime() < this.retry_after.getOrDefault(slot, 0L)) continue;
      startProposal(slot, item.getValue(), ballot);
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
        if (this.fast_active && slot >= this.any_from) break;
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
    var collector =
        new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(
            new ArrayList<>(), acceptors.size(), processor);
    this.in_flight.put(slot, new Proposal(value, processor, collector));
    var request_builder =
        DidaTradePaxos.PhaseTwoRequest.newBuilder()
            .setInstance(slot)
            .setValue(value)
            .setRequestballot(ballot);
    RequestRecord record = this.server_state.req_history.getIfExists(value);
    if (record != null && this.server_state.scheduler.needsActivation(ballot))
      request_builder.setCommand(record.getRequest().toMessage());
    var request = request_builder.build();
    System.out.println(
        "Sending phase 2 for slot " + slot + " in ballot " + ballot + ", value " + value);
    for (int acceptor : acceptors)
      this.server_state.async_stubs[acceptor].phasetwo(request, observer(collector));
  }

  private void startAny(int ballot) {
    List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
    this.any_collector =
        new GenericResponseCollector<DidaTradePaxos.AnyReply>(
            new ArrayList<DidaTradePaxos.AnyReply>(), acceptors.size());

    var request =
        DidaTradePaxos.AnyRequest.newBuilder()
            .setInstance(this.any_from)
            .setRequestballot(ballot)
            .build();

    System.out.println("Sending 'any' from slot " + this.any_from + " in ballot " + ballot);

    this.any_sent = true;
    for (int acceptor : acceptors)
      this.server_state.async_stubs[acceptor].any(request, observer(this.any_collector));
  }

  private void finishAny(int ballot) {
    if (this.any_collector == null || !this.any_collector.isDone()) return;

    int max_seen = -1;
    int accepted_count = 0;
    for (DidaTradePaxos.AnyReply reply : this.any_collector.getCollectedResponses()) {
      if (reply.getAccepted()) accepted_count++;
      else if (reply.getMaxballot() > max_seen) max_seen = reply.getMaxballot();
    }
    synchronized (this.server_state) {
      if (max_seen > -1) this.server_state.setCurrentBallot(max_seen);
    }

    // in case 'any' message didn't reach all replicas, we retry after a timeout
    if (max_seen == -1 && accepted_count < this.server_state.scheduler.acceptors(ballot).size()) {
      this.any_sent = false;
      this.any_retry_after = retryLater();
    }

    this.any_collector = null;
  }

  private void fastAccept(int ballot) {
    Map<Integer, Integer> placed = new HashMap<>();

    synchronized (this.server_state) {
      if (ballot != this.server_state.getCurrentBallot()
          || this.server_state.getAnyBallot() != ballot
          || this.server_state.scheduler.isacceptor(this.server_state.my_id, ballot) == false) {
        return;
      }

      // start fast-accepting only after executing the recovered prefix
      if (this.next_log_entry < this.server_state.getAnyFrom()) return;

      // release requests whose slot was won by another request
      var iterator = this.fast_placed.entrySet().iterator();
      while (iterator.hasNext()) {
        var placement = iterator.next();
        int request = placement.getKey();
        int slot = placement.getValue();

        PaxosInstance entry = this.server_state.paxos_log.getEntry(slot);
        if (entry != null && entry.decided && entry.command_id != request) iterator.remove();
      }

      // requests already decided in the window must not be voted for again
      Set<Integer> decided = new HashSet<>();
      for (int s = this.next_log_entry; s < this.next_log_entry + PIPELINE_WINDOW; s++) {
        PaxosInstance e = this.server_state.paxos_log.getEntry(s);
        if (e != null && e.decided) decided.add(e.command_id);
      }

      int slot = Math.max(this.server_state.getAnyFrom(), this.next_log_entry);
      for (int reqid : this.server_state.req_history.getPendingIds()) {
        if (this.fast_placed.containsKey(reqid)) continue;
        if (decided.contains(reqid)) continue;

        PaxosInstance entry = this.server_state.paxos_log.getEntry(slot);
        // finds available slot to place the request
        while (entry != null && (entry.write_ballot == ballot || entry.decided)) {
          slot++;
          entry = this.server_state.paxos_log.getEntry(slot);
        }

        if (slot >= this.next_log_entry + PIPELINE_WINDOW)
          break; // in case we keep the pipeline window

        entry = this.server_state.paxos_log.testAndSetEntry(slot);
        entry.accepted_value = reqid;
        entry.write_ballot = ballot;
        entry.accepted_fast = true;
        this.fast_placed.put(reqid, slot);
        placed.put(reqid, slot);
        System.out.println(
            "Fast accepted request " + reqid + " in slot " + slot + " in ballot " + ballot);
        slot++;
      }
    }

    for (Map.Entry<Integer, Integer> item : placed.entrySet()) {
      int reqid = item.getKey();
      int slot = item.getValue();
      this.server_state.sendLearn(slot, ballot, reqid, true);
    }
  }

  private void executeReadyEntries() {
    while (true) {
      // Publish each command's effects and its executed index together.
      synchronized (this.server_state) {
        PaxosInstance entry = this.server_state.paxos_log.getEntry(this.next_log_entry);
        if (entry == null || !entry.decided) return;
        if (entry.command_id != PaxosInstance.NO_OP
            && this.server_state.req_history.getIfProcessed(entry.command_id) == null) {
          RequestRecord request = this.server_state.req_history.getIfPending(entry.command_id);
          if (request == null) return;
          execute(entry, request);
        }
        System.out.println(
            "Log entry with number "
                + this.next_log_entry
                + " has been decided with command id = "
                + entry.command_id);
        this.prepared_values.remove(this.next_log_entry);
        // Learning/executing locally does not confirm transfer to the new quorum.
        if (!this.transfer_remaining.containsKey(this.next_log_entry))
          this.in_flight.remove(this.next_log_entry);
        this.retry_after.remove(this.next_log_entry);
        this.next_log_entry++;
        this.server_state.setExecutedIndex(this.next_log_entry);
      }
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
    request_record.setLogIndex(this.next_log_entry + 1);
    request_record.setResponse(result);
    this.server_state.req_history.moveToProcessed(request_record.getId());
  }
}
