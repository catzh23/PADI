package didatrade.server;

import java.util.HashSet;
import java.util.Set;

public class PaxosInstance {
  // Client request IDs start at 101; zero denotes an internal no-op.
  static final int NO_OP = 0;
  int instance_nb;
  volatile int command_id;
  int accepted_value;
  int learning_value;
  int read_ballot;
  int write_ballot;
  int accept_ballot;
  final Set<Integer> learning_acceptors = new HashSet<>();
  volatile boolean decided;
  boolean value_is_locked;

  public PaxosInstance() {
    this.instance_nb = 0;
    this.command_id = 0;
    this.read_ballot = -1;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.decided = false;
    this.value_is_locked = false;
  }

  public PaxosInstance(int id) {
    this.instance_nb = id;
    this.command_id = 0;
    this.read_ballot = -1;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.decided = false;
    this.value_is_locked = false;
  }

  public PaxosInstance(int id, int ballot) {
    this.instance_nb = id;
    this.command_id = 0;
    this.read_ballot = ballot;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.decided = false;
    this.value_is_locked = false;
  }
}
