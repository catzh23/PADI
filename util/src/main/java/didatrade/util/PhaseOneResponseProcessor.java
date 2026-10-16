package didatrade.util;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Real phase 1. Wait quorum. Keep value with highest valballot. */
public class PhaseOneResponseProcessor extends PhaseOneProcessor {

  private final int old_quorum;
  private final int new_quorum;
  private final Set<Integer> old_acceptors;
  private final Set<Integer> new_acceptors;
  private final Set<Integer> expected_acceptors;
  private final Map<Integer, DidaTradePaxos.CommandData> commands = new HashMap<>();

  // Set not counter. Same acceptor twice must not fill quorum twice.
  private final Set<Integer> promises;
  private final Map<Integer, DidaTradePaxos.AcceptedInstance> acceptedInstances = new HashMap<>();

  private boolean rejected;
  private int value;
  private int valballot;
  private int maxballot;

  private final ConfigurationScheduler scheduler;
  private final Map<Integer, Map<Integer, Integer>> slotVotes = new HashMap<>(); // slot -> votes

  public PhaseOneResponseProcessor(ConfigurationScheduler s, int low_ballot, int high_ballot) {
    this.scheduler = s;
    this.old_acceptors = new HashSet<>(s.acceptors(low_ballot));
    this.old_quorum = s.quorum(low_ballot);
    this.new_acceptors = new HashSet<>();
    // Schedule C has two acceptor groups. An unfinished intermediate ballot
    // may already have decided slots in the second group, before activation.
    if (s.needsActivation(high_ballot) && high_ballot > low_ballot) {
      Set<Integer> previous_acceptors = new HashSet<>(s.acceptors(high_ballot - 1));
      if (!previous_acceptors.equals(this.old_acceptors))
        this.new_acceptors.addAll(previous_acceptors);
    }
    this.new_quorum = this.new_acceptors.isEmpty() ? 0 : s.quorum(high_ballot - 1);
    this.expected_acceptors = new HashSet<>(this.old_acceptors);
    this.expected_acceptors.addAll(this.new_acceptors);
    this.promises = new HashSet<Integer>();
    this.rejected = false;
    this.value = -1;
    // -1 mean nobody accept anything yet. Leader then free to propose own request.
    this.valballot = -1;
    this.maxballot = -1;
  }

  public PhaseOneResponseProcessor(ConfigurationScheduler s, int reference_ballot) {
    this(s, reference_ballot, reference_ballot);
  }

  public synchronized ArrayList<Integer> getAcceptors() {
    return new ArrayList<>(this.expected_acceptors);
  }

  private boolean hasEnoughPromises() {
    int old_count = 0;
    int new_count = 0;
    for (int acceptor : this.promises) {
      if (this.old_acceptors.contains(acceptor)) old_count++;
      if (this.new_acceptors.contains(acceptor)) new_count++;
    }
    // A shared acceptor counts once in each group. A majority of the union is not enough.
    return old_count >= this.old_quorum && new_count >= this.new_quorum;
  }

  // Derived, not stored field. addNoResponse() never reach processor, so dead or frozen
  // acceptor give no reply and no callback. Stored `accepted = true` would report quorum
  // never got. That is the bogus bug.
  @Override
  public synchronized boolean getAccepted() {
    return !this.rejected && hasEnoughPromises();
  }

  @Override
  public synchronized int getValue() {
    return this.value;
  }

  @Override
  public synchronized int getValballot() {
    return this.valballot;
  }

  @Override
  public synchronized int getMaxballot() {
    return this.maxballot;
  }

  @Override
  public synchronized Map<Integer, DidaTradePaxos.AcceptedInstance> getAcceptedInstances() {
    Map<Integer, DidaTradePaxos.AcceptedInstance> result = new HashMap<>();

    for (DidaTradePaxos.AcceptedInstance entry : this.acceptedInstances.values()) {
      int slot = entry.getInstance();
      int threshold = this.scheduler.recoveryquorum(entry.getValballot());

      DidaTradePaxos.AcceptedInstance chosen = entry; // "otherwise": any value with ballot j
      for (Map.Entry<Integer, Integer> vote : this.slotVotes.get(slot).entrySet()) {
        if (vote.getValue() >= threshold) {
          // value may have been decided in ballot j: must adopt it.
          chosen = entry.toBuilder().setValue(vote.getKey()).build();
        }
      }
      var builder = chosen.toBuilder().clearCommand();
      if (this.commands.containsKey(chosen.getValue()))
        builder.setCommand(this.commands.get(chosen.getValue()));
      result.put(slot, builder.build());
    }
    return Map.copyOf(result);
  }

  /** Promise count. For logs and report message counts. */
  public synchronized int getPromises() {
    return this.promises.size();
  }

  /** Return true when leader heard enough. Collector then stop waiting. */
  @Override
  public synchronized boolean onNext(
      ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
      DidaTradePaxos.PhaseOneReply last_response) {

    if (!this.expected_acceptors.contains(last_response.getServerid())) return false;

    // Track highest ballot even on reject. Leader use it to catch up.
    if (last_response.getMaxballot() > this.maxballot) {
      this.maxballot = last_response.getMaxballot();
    }

    // Acceptor promised higher ballot. This ballot dead. Stop now.
    if (last_response.getAccepted() == false) {
      this.rejected = true;
      return true;
    }

    // Duplicate reply from acceptor already counted. Ignore.
    if (this.promises.add(last_response.getServerid()) == false) {
      return false;
    }

    for (DidaTradePaxos.AcceptedInstance entry : last_response.getAcceptedinstancesList()) {
      if (entry.getValballot() < 0) continue;
      if (entry.hasCommand()) this.commands.put(entry.getValue(), entry.getCommand());

      int slot = entry.getInstance();

      DidaTradePaxos.AcceptedInstance previous = this.acceptedInstances.get(slot);
      if (previous == null || entry.getValballot() > previous.getValballot()) {
        this.acceptedInstances.put(slot, entry); // new highest ballot
        this.slotVotes.put(slot, new HashMap<>()); // so restart its counts
      }

      if (entry.getValballot() == this.acceptedInstances.get(slot).getValballot()) {
        this.slotVotes
            .get(slot)
            .merge(entry.getValue(), 1, Integer::sum); // count votes for this value
      }
    }

    // Keep value accepted at highest ballot. This rule stop new leader overwriting
    // value older ballot may already have decided.
    if (last_response.getValballot() > this.valballot) {
      this.valballot = last_response.getValballot();
      this.value = last_response.getValue();
    }

    return hasEnoughPromises();
  }
}
