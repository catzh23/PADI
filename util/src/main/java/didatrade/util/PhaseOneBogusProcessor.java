package didatrade.util;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import java.util.ArrayList;

public class PhaseOneBogusProcessor extends PhaseOneProcessor {
  private ConfigurationScheduler scheduler;
  private boolean accepted;
  private int value;
  private int valballot;
  private int maxballot;
  private int low_ballot;
  private int high_ballot;

  public PhaseOneBogusProcessor(ConfigurationScheduler s, int l, int h) {
    // Defect 1. Stored true, never reassigned. Rejected ballot still report accepted.
    this.accepted = true;
    this.value = -1;
    this.valballot = -1;
    this.maxballot = -1;
    this.low_ballot = l;
    this.high_ballot = h;
    this.scheduler = s;
  }

  @Override
  public boolean getAccepted() {
    return this.accepted;
  }

  @Override
  public int getValue() {
    return this.value;
  }

  @Override
  public int getValballot() {
    return this.valballot;
  }

  @Override
  public int getMaxballot() {
    return this.maxballot;
  }

  @Override
  public synchronized boolean onNext(
      ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
      DidaTradePaxos.PhaseOneReply last_response) {
    // Defect 2. Overwrite every field from last reply. No valballot compare, so value
    // accepted at older ballot get lost.
    this.maxballot = last_response.getMaxballot();
    this.value = last_response.getValue();
    this.valballot = last_response.getValballot();
    // Defect 3. Done after one reply. Never wait quorum.
    return true;
  }
}
