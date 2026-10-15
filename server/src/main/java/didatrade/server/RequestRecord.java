package didatrade.server;

public class RequestRecord {
  private int requestid;
  private DidaTradeCommand request;
  private boolean response_available;
  private boolean response_value;
  private int log_index;

  public RequestRecord(int id) {
    this.requestid = id;
    this.request = null;
    this.response_available = false;
    this.response_value = false;
    this.log_index = 0;
  }

  public RequestRecord(int id, DidaTradeCommand rq) {
    this.requestid = id;
    this.request = rq;
    this.response_available = false;
    this.response_value = false;
    this.log_index = 0;
  }

  // Getter and Setter methods for all fields
  public DidaTradeCommand getRequest() {
    return this.request;
  }

  public int getId() {
    return this.requestid;
  }

  public void setRequest(DidaTradeCommand rq) {
    this.request = rq;
  }

  public int getLogIndex() {
    return this.log_index;
  }

  public void setLogIndex(int index) {
    this.log_index = index;
  }

  public boolean getResponseValue() {
    return this.response_value;
  }

  public synchronized void setResponse(boolean resp) {
    this.response_value = resp;
    this.response_available = true;
    this.notifyAll();
  }

  public synchronized boolean waitForResponse() {
    while (this.response_available == false) {
      try {
        wait();
      } catch (InterruptedException e) {
      }
    }
    return this.response_value;
  }
}
