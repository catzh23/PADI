package didatrade;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.concurrent.ThreadLocalRandom;

public class DebugInterceptor implements ServerInterceptor {

  private static final int SLOW_MIN_DELAY_MS = 500;
  private static final int SLOW_MAX_DELAY_MS = 4000;

  private boolean frozen = false;
  private boolean slow = false;

  public DebugInterceptor() {}

  public synchronized void freeze() {
    this.frozen = true;
  }

  public synchronized void unfreeze() {
    this.frozen = false;
    notifyAll();
  }

  public synchronized void setSlow(boolean slow) {
    this.slow = slow;
  }

  public synchronized boolean isFrozen() {
    return this.frozen;
  }

  public synchronized boolean isSlow() {
    return this.slow;
  }

  public synchronized boolean waitIfFrozen() {
    while (this.frozen) {
      try {
        wait();
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return this.slow;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (waitIfFrozen()) {
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(SLOW_MIN_DELAY_MS, SLOW_MAX_DELAY_MS + 1));
      } catch (InterruptedException e) {
      }
    }
    return next.startCall(call, headers);
  }
}
