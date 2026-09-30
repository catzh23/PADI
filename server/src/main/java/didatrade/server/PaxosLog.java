package didatrade.server;

import didatrade.DidaTradePaxos;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class PaxosLog {
  private Hashtable<Integer, PaxosInstance> log;

  public PaxosLog() {
    this.log = new Hashtable<Integer, PaxosInstance>();
  }

  public synchronized int length() {
    return this.log.size();
  }

  /** Caller holds server_state's monitor, also used by acceptors when writing. */
  public synchronized List<DidaTradePaxos.AcceptedInstance> acceptedInstances(int firstInstance) {
    List<DidaTradePaxos.AcceptedInstance> entries = new ArrayList<>();
    for (PaxosInstance entry : this.log.values()) {
      if (entry.instance_nb >= firstInstance && entry.write_ballot >= 0) {
        entries.add(
            DidaTradePaxos.AcceptedInstance.newBuilder()
                .setInstance(entry.instance_nb)
                .setValue(entry.accepted_value)
                .setValballot(entry.write_ballot)
                .build());
      }
    }
    return entries;
  }

  public synchronized PaxosInstance getEntry(int position) {
    return this.log.get(position);
  }

  public synchronized PaxosInstance testAndSetEntry(int position) {
    PaxosInstance entry = this.log.get(position);

    if (entry == null) {
      entry = new PaxosInstance(position);
      this.log.put(position, entry);
    }
    return entry;
  }

  public synchronized PaxosInstance testAndSetEntry(int position, int ballot) {
    PaxosInstance entry = this.log.get(position);

    if (entry == null) {
      entry = new PaxosInstance(position, ballot);
      this.log.put(position, entry);
    }
    return entry;
  }
}
