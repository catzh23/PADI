package didatrade.server;

import didatrade.DebugInterceptor;
import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DidaTradeMasterServiceImpl
    extends DidaTradeMasterServiceGrpc.DidaTradeMasterServiceImplBase {
  DidaTradeServerState server_state;

  public DidaTradeMasterServiceImpl(DidaTradeServerState state) {
    this.server_state = state;
  }

  @Override
  public void newballot(
      DidaTradeMaster.NewBallotRequest request,
      StreamObserver<DidaTradeMaster.NewBallotReply> responseObserver) {
    System.out.println(request);

    int request_id = request.getReqid();
    int new_ballot = request.getNewballot();
    int completed_ballot = request.getCompletedballot();
    ;

    // for debug purposes
    System.out.println(
        "Current ballot = "
            + this.server_state.getCurrentBallot()
            + " new ballot = "
            + new_ballot
            + " completed ballot = "
            + completed_ballot);

    this.server_state.setCompletedBallot(completed_ballot);

    if (new_ballot > this.server_state.getCurrentBallot()) {
      this.server_state.setCurrentBallot(new_ballot);

      this.server_state.main_loop.wakeup();

      completed_ballot = this.server_state.waitForCompletedBallot(new_ballot);
    } else {
      completed_ballot = this.server_state.getCompletedBallot();
    }

    DidaTradeMaster.NewBallotReply.Builder response_builder =
        DidaTradeMaster.NewBallotReply.newBuilder();
    response_builder.setReqid(request_id);
    response_builder.setCompletedballot(completed_ballot);

    DidaTradeMaster.NewBallotReply response = response_builder.build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void activate(
      DidaTradeMaster.ActivateRequest request,
      StreamObserver<DidaTradeMaster.ActivateReply> responseObserver) {
    System.out.println(request);

    int request_id = request.getReqid();
    int completed_ballot = request.getCompletedballot();
    ;

    // for debug purposes
    System.out.println(
        "Current ballot = "
            + this.server_state.getCurrentBallot()
            + " activated ballot = "
            + completed_ballot);

    // do stuff

    DidaTradeMaster.ActivateReply.Builder response_builder =
        DidaTradeMaster.ActivateReply.newBuilder();
    response_builder.setReqid(request_id);
    response_builder.setAck(true);

    DidaTradeMaster.ActivateReply response = response_builder.build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void setdebug(
      DidaTradeMaster.SetDebugRequest request,
      StreamObserver<DidaTradeMaster.SetDebugReply> responseObserver) {
    // for debug purposes
    System.out.println(request);
    int request_id = request.getReqid();
    int mode = request.getMode();
    boolean response_value = true;

    switch (mode) {
      case DebugInterceptor.FAIL:
        break; // exits below, after the reply is sent
      case DebugInterceptor.FREEZE:
        this.server_state.debug_interceptor.freeze();
        break;
      case DebugInterceptor.UNFREEZE:
        this.server_state.debug_interceptor.unfreeze();
        break;
      case DebugInterceptor.SLOW:
        this.server_state.debug_interceptor.setSlow(true);
        break;
      case DebugInterceptor.FAST:
        this.server_state.debug_interceptor.setSlow(false);
        break;
      default:
        response_value = false;
        break;
    }

    if (response_value) this.server_state.setDebugMode(mode);

    // for debug purposes
    System.out.println("Setting debug mode to = " + this.server_state.getDebugMode());

    DidaTradeMaster.SetDebugReply.Builder response_builder =
        DidaTradeMaster.SetDebugReply.newBuilder();
    response_builder.setReqid(request_id);
    response_builder.setAck(response_value);

    DidaTradeMaster.SetDebugReply response = response_builder.build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
    if (mode == DebugInterceptor.FAIL) {
      new Thread(
              () -> {
                try {
                  Thread.sleep(200); // let the reply reach the console
                } catch (InterruptedException e) {
                }
                System.exit(1);
              })
          .start();
    }
  }
}
