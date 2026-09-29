package didatrade.util;

import didatrade.DidaTradePaxos;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class PhaseTwoResponseProcessor
    extends GenericResponseProcessor<DidaTradePaxos.PhaseTwoReply> {
  private boolean rejected;
  private int maxballot;
  private final int quorum;
  private final Set<Integer> acceptors;

  public PhaseTwoResponseProcessor(int q) {
    this.rejected = false;
    this.maxballot = 0;
    this.quorum = q;
    this.acceptors = new HashSet<Integer>();
  }

  public synchronized boolean getAccepted() {
    return !this.rejected && this.acceptors.size() >= this.quorum;
  }

  public synchronized int getMaxballot() {
    return this.maxballot;
  }

  public synchronized boolean onNext(
      ArrayList<DidaTradePaxos.PhaseTwoReply> all_responses,
      DidaTradePaxos.PhaseTwoReply last_response) {
    if (last_response.getAccepted() == false) {
      this.rejected = true;
      if (last_response.getMaxballot() > this.maxballot)
        this.maxballot = last_response.getMaxballot();
      return true;
    }
    this.acceptors.add(last_response.getServerid());
    return this.acceptors.size() >= this.quorum;
  }
}
