import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import didatrade.util.*;
import java.util.ArrayList;

/** Standalone checks against the compiled project; does not modify application code. */
public class ProcessorChecks {
  static int failures = 0;
  static void check(String name, boolean condition) {
    System.out.println((condition ? "PASS " : "FAIL ") + name);
    if (!condition) failures++;
  }
  static DidaTradePaxos.PhaseOneReply reply(int id, int vb, int value, boolean ok) {
    return DidaTradePaxos.PhaseOneReply.newBuilder().setServerid(id)
        .setAccepted(ok).setValballot(vb).setValue(value).setMaxballot(ok ? 4 : 5).build();
  }
  public static void main(String[] args) {
    var schedule = new ConfigurationScheduler('A');
    var p = new PhaseOneResponseProcessor(schedule, 0, 4);
    var replies = new ArrayList<DidaTradePaxos.PhaseOneReply>();
    check("phase1: no replies is not quorum", !p.getAccepted());
    p.onNext(replies, reply(0, -1, 0, true));
    check("phase1: one promise is not quorum", !p.getAccepted());
    p.onNext(replies, reply(0, -1, 0, true));
    check("phase1: duplicate acceptor is not quorum", !p.getAccepted());
    p.onNext(replies, reply(2, -1, 0, true));
    check("phase1: two distinct promises, no previous value", p.getAccepted() && p.getValballot() == -1);
    for (boolean reverse : new boolean[]{false, true}) {
      p = new PhaseOneResponseProcessor(schedule, 0, 4);
      p.onNext(replies, reply(0, reverse ? 3 : 1, reverse ? 303 : 101, true));
      p.onNext(replies, reply(2, reverse ? 1 : 3, reverse ? 101 : 303, true));
      check("phase1: highest accepted ballot, reverse=" + reverse,
          p.getAccepted() && p.getValballot() == 3 && p.getValue() == 303);
    }
    p = new PhaseOneResponseProcessor(schedule, 0, 4);
    p.onNext(replies, reply(0, -1, 0, true));
    p.onNext(replies, reply(1, -1, 0, false));
    check("phase1: rejection aborts and reports newer ballot", !p.getAccepted() && p.getMaxballot() == 5);
    p = new PhaseOneResponseProcessor(schedule, 0, 4);
    var c = new GenericResponseCollector<DidaTradePaxos.PhaseOneReply>(new ArrayList<>(), 3, p);
    c.addResponse(reply(0, -1, 0, true)); c.addNoResponse(); c.addNoResponse(); c.waitUntilDone();
    check("phase1: one success plus two RPC failures is not quorum", !p.getAccepted());

    var p2 = new PhaseTwoResponseProcessor(2);
    var c2 = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(new ArrayList<>(), 3, p2);
    c2.addResponse(DidaTradePaxos.PhaseTwoReply.newBuilder().setServerid(0).setAccepted(true).build());
    c2.addNoResponse(); c2.addNoResponse(); c2.waitUntilDone();
    check("phase2: one success plus two RPC failures MUST NOT be quorum", !p2.getAccepted());
    p2 = new PhaseTwoResponseProcessor(2);
    c2 = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(new ArrayList<>(), 3, p2);
    c2.addNoResponse(); c2.addNoResponse(); c2.addNoResponse(); c2.waitUntilDone();
    check("phase2: zero successes MUST NOT be quorum", !p2.getAccepted());
    p2 = new PhaseTwoResponseProcessor(2);
    c2 = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(new ArrayList<>(), 3, p2);
    check("phase2: starts without quorum", !p2.getAccepted());
    var accepted0 = DidaTradePaxos.PhaseTwoReply.newBuilder().setServerid(0).setAccepted(true).build();
    var accepted1 = DidaTradePaxos.PhaseTwoReply.newBuilder().setServerid(1).setAccepted(true).build();
    c2.addResponse(accepted0);
    check("phase2: one acceptance is not quorum", !p2.getAccepted());
    c2.addResponse(accepted0);
    check("phase2: duplicate acceptor is not quorum", !p2.getAccepted());
    c2.addResponse(accepted1);
    c2.waitUntilDone();
    check("phase2: two distinct acceptors reach quorum", p2.getAccepted());
    p2 = new PhaseTwoResponseProcessor(2);
    c2 = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(new ArrayList<>(), 3, p2);
    c2.addNoResponse();
    c2.addResponse(accepted0);
    c2.addResponse(accepted1);
    c2.waitUntilDone();
    check("phase2: two acceptances succeed despite one RPC failure", p2.getAccepted());
    p2 = new PhaseTwoResponseProcessor(2);
    c2 = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(new ArrayList<>(), 3, p2);
    c2.addResponse(accepted0);
    c2.addResponse(DidaTradePaxos.PhaseTwoReply.newBuilder().setServerid(1)
        .setAccepted(false).setMaxballot(5).build());
    c2.waitUntilDone();
    check("phase2: rejection aborts and reports newer ballot", !p2.getAccepted() && p2.getMaxballot() == 5);
    c2.addResponse(accepted1);
    check("phase2: late acceptance cannot undo aborted attempt", !p2.getAccepted());
    System.out.println("Failed checks: " + failures);
    System.exit(failures == 0 ? 0 : 1);
  }
}
