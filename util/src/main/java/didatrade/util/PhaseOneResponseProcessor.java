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

  private final int quorum;

  // Set not counter. Same acceptor twice must not fill quorum twice.
  private final Set<Integer> promises;
  private final Map<Integer, DidaTradePaxos.AcceptedInstance> acceptedInstances = new HashMap<>();

  private boolean rejected;
  private int value;
  private int valballot;
  private int maxballot;

  public PhaseOneResponseProcessor(ConfigurationScheduler s, int low_ballot, int high_ballot) {
    this.quorum = s.quorum(high_ballot);
    this.promises = new HashSet<Integer>();
    this.rejected = false;
    this.value = -1;
    // -1 mean nobody accept anything yet. Leader then free to propose own request.
    this.valballot = -1;
    this.maxballot = -1;
  }

  // Derived, not stored field. addNoResponse() never reach processor, so dead or frozen
  // acceptor give no reply and no callback. Stored `accepted = true` would report quorum
  // never got. That is the bogus bug.
  @Override
  public synchronized boolean getAccepted() {
    return !this.rejected && this.promises.size() >= this.quorum;
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
    return Map.copyOf(this.acceptedInstances);
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
      DidaTradePaxos.AcceptedInstance previous = this.acceptedInstances.get(entry.getInstance());
      if (entry.getValballot() >= 0
          && (previous == null || entry.getValballot() > previous.getValballot())) {
        this.acceptedInstances.put(entry.getInstance(), entry);
      }
    }

    // Keep value accepted at highest ballot. This rule stop new leader overwriting
    // value older ballot may already have decided.
    if (last_response.getValballot() > this.valballot) {
      this.valballot = last_response.getValballot();
      this.value = last_response.getValue();
    }

    return this.promises.size() >= this.quorum;
  }
}
