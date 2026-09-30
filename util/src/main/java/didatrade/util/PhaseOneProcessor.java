package didatrade.util;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import java.util.Map;

/**
 * Common type for phase 1 processors. MainLoop hold either one.
 *
 * <p>Two exist on purpose. PhaseOneResponseProcessor is real one, used by default.
 * PhaseOneBogusProcessor is broken baseline, kept so Step 1 safety violation stay reproducible for
 * report and discussion.
 */
public abstract class PhaseOneProcessor
    extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {

  /**
   * System property picking implementation. "bogus" select broken baseline. Pick it with
   * -Ddidatrade.phase1=bogus.
   */
  public static final String MODE_PROPERTY = "didatrade.phase1";

  /** True when quorum of distinct acceptors promised ballot and none rejected. */
  public abstract boolean getAccepted();

  /** Value to carry into phase 2. Valid only when getValballot() > -1. */
  public abstract int getValue();

  /** Ballot that accepted getValue(). -1 when no value found. */
  public abstract int getValballot();

  /** Highest ballot any acceptor reported. Leader use it to catch up after reject. */
  public abstract int getMaxballot();

  /** Highest-ballot acceptance recovered for each instance in the quorum. */
  public abstract Map<Integer, DidaTradePaxos.AcceptedInstance> getAcceptedInstances();

  public static PhaseOneProcessor create(
      ConfigurationScheduler s, int low_ballot, int high_ballot) {
    if ("bogus".equalsIgnoreCase(System.getProperty(MODE_PROPERTY, ""))) {
      System.out.println("*** phase 1: using the BOGUS processor (demo mode) ***");
      return new PhaseOneBogusProcessor(s, low_ballot, high_ballot);
    }
    return new PhaseOneResponseProcessor(s, low_ballot, high_ballot);
  }
}
