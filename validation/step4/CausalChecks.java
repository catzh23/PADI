package didatrade.server;

import didatrade.DidaTradeMain;
import didatrade.core.TradeManager;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Pause a write after changing the balance, before publishing its log index. */
public class CausalChecks {
  static int failures;

  static void check(String name, boolean ok) {
    System.out.println((ok ? "PASS " : "FAIL ") + name);
    if (!ok) failures++;
  }

  static class PausedTradeManager extends TradeManager {
    final CountDownLatch changed = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);

    @Override
    public boolean acquire(int user, int fraction) {
      boolean result = super.acquire(user, fraction);
      changed.countDown();
      try {
        if (!resume.await(5, TimeUnit.SECONDS)) throw new AssertionError("write was not released");
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      return result;
    }
  }

  static class ObservedRequest extends RequestRecord {
    int index_when_response_ready = -1;

    ObservedRequest() {
      super(101, new DidaTradeCommand(DidaTradeAction.BUY, 0, 50));
    }

    @Override
    public synchronized void setResponse(boolean result) {
      index_when_response_ready = getLogIndex();
      super.setResponse(result);
    }
  }

  public static void main(String[] args) throws Exception {
    var state = new DidaTradeServerState(29890, 1, 'A');
    state.main_loop_worker.interrupt();
    state.main_loop_worker.join(2000);
    var manager = new PausedTradeManager();
    manager.populate(10);
    state.trade_manager = manager;
    var request = new ObservedRequest();
    state.req_history.addToPending(101, request);
    var entry = state.paxos_log.testAndSetEntry(0);
    entry.command_id = 101;
    entry.decided = true;
    var execute = MainLoop.class.getDeclaredMethod("executeReadyEntries");
    execute.setAccessible(true);
    var error = new AtomicReference<Throwable>();
    var response = new AtomicReference<DidaTradeMain.BalanceReply>();
    Thread writer = new Thread(() -> {
      try { execute.invoke(state.main_loop); }
      catch (Throwable e) { error.set(e); }
    });
    Thread reader = new Thread(() -> {
      try {
        new DidaTradeMainServiceImpl(state).balance(
            DidaTradeMain.BalanceRequest.newBuilder().setReqid(201).setUserid(0).build(),
            new StreamObserver<DidaTradeMain.BalanceReply>() {
              public void onNext(DidaTradeMain.BalanceReply value) { response.set(value); }
              public void onError(Throwable e) { error.set(e); }
              public void onCompleted() {}
            });
      } catch (Throwable e) { error.set(e); }
    });
    try {
      writer.start();
      if (!manager.changed.await(5, TimeUnit.SECONDS)) throw new AssertionError("write did not start");
      reader.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (reader.isAlive() && reader.getState() != Thread.State.BLOCKED
          && System.nanoTime() < deadline) Thread.sleep(5);
      check("causal: read waits while balance changed but index is not published",
          reader.getState() == Thread.State.BLOCKED && response.get() == null);
      manager.resume.countDown();
      writer.join(5000);
      reader.join(5000);
      check("causal: write response already contains its index",
          request.index_when_response_ready == 1);
      check("causal: read returns the new balance with its matching index",
          response.get() != null && response.get().getBalance() == 5
              && response.get().getLastindex() == 1);
      check("causal: threads finish without errors",
          !writer.isAlive() && !reader.isAlive() && error.get() == null);
    } finally {
      manager.resume.countDown();
      writer.interrupt();
      reader.interrupt();
      for (var channel : state.channels) channel.shutdownNow();
    }
    System.exit(failures == 0 ? 0 : 1);
  }
}
