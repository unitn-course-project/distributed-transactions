package it.unitn.ds1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.TxnCoordinator.Decision;
import it.unitn.ds1.TxnCoordinator.DecisionRequest;
import it.unitn.ds1.TxnCoordinator.DecisionResponse;
import it.unitn.ds1.TxnCoordinator.ReadDataMsg;
import it.unitn.ds1.TxnCoordinator.ReadDataResultMsg;
import it.unitn.ds1.TxnCoordinator.Vote;
import it.unitn.ds1.TxnCoordinator.VoteReponse;
import it.unitn.ds1.TxnCoordinator.VoteRequest;
import it.unitn.ds1.model.RowValue;
import scala.concurrent.duration.Duration;

/**
 * TxnServer is a actor in order to manipulate data
 */
public class TxnServer extends Node {
  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  // data
  private Map<Integer, RowValue> data;

  // operated variables
  private Map<Integer, String> validationLocks;
  private Map<String, Map<Integer, Integer>> transactionChange;
  private Map<String, ActorRef> mapTransactionCoordinator;

  public TxnServer(int id) {
    super(id);
  }

  static public Props props(int id) {
    return Props.create(TxnServer.class, () -> new TxnServer(id));
  }

  @Override
  public void preStart() {
    data = new HashMap<>();
    // init data
    for (int i = 10 * id; i < 10 * (id + 1); i++) {
      data.put(i, new RowValue(0, 100));
    }
    validationLocks = new HashMap<>();
    transactionChange = new HashMap<>();
    mapTransactionCoordinator = new HashMap<>();
  }

  /*-- Message classes ------------------------------------------------------ */
  /**
   * Message caculate the correctness of the system
   */
  public static class SumTestRequest implements Serializable {
    public final int testId;

    public SumTestRequest(int testId) {
      this.testId = testId;
    }

  }

  public static class Timeout implements Serializable {
    public final String transactionId;

    public Timeout(String transactionId) {
      this.transactionId = transactionId;
    }

  }

  /*-- Message handlers ----------------------------------------------------- */
  /**
   * Handle read message from coordinator
   * 
   * @param readMsg
   */
  private void onReadMsg(ReadDataMsg readMsg) {
    RowValue readValue = data.get(readMsg.key);
    // send back the coordinator data by key
    getSender().tell(
        new ReadDataResultMsg(readMsg.transactionId, readMsg.key, readValue.getValue(), readValue.getVersion()),
        getSelf());
  }

  /**
   * Handle timeout event
   * 
   * @param timeout
   */
  private void onTimeout(Timeout timeout) {
    if (transactionChange.containsKey(timeout.transactionId)) {
      // re-sent DecisionRequest
      mapTransactionCoordinator.get(timeout.transactionId).tell(new DecisionRequest(timeout.transactionId), getSelf());
      // set timeout again
      setTimeout(timeout.transactionId, TxnSystem.DECISION_TIMEOUT);
    }
  }

  /**
   * Handle Sumtest Request for calculate sum in order to check correctness of
   * system
   * 
   * @param sumTestRequest
   */
  private void onSumTestRequest(SumTestRequest sumTestRequest) {
    Integer sum = 0;
    for (Integer key : data.keySet()) {
      sum += data.get(key).getValue();
    }
    log.info("Sum test server :" + id + " test id is " + sumTestRequest.testId + " sum =" + sum);
    BufferedWriter writer;
    try {
      writer = new BufferedWriter(new FileWriter(TxnSystem.LOG_SUM_FILENAME, true));
      // writer.write("Sum test server :" + id + " test id is " +
      // sumTestRequest.testId + " sum =" + sum + "\n");
      writer.write(sumTestRequest.testId + ", " + sum + "\n");
      // writer.write(sum + "\n");

      writer.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Handle vote request and validate changes
   * 
   * @param vRequest
   */
  private void onVoteRequest(VoteRequest vRequest) {
    Random r = new Random();
    if (r.nextDouble() < TxnSystem.CRASH_PROBABILITY)
      if (this.id == 0) {
        crash(TxnSystem.CRASH_TIME);
        return;
      }
    Map<Integer, RowValue> changeData = vRequest.changes;
    Map<Integer, Integer> changes = new HashMap<>();
    for (Integer key : changeData.keySet())
      // Check lock and data version if it violate version contraint or locks, then
      // abort transaction
      if (validationLocks.containsKey(key) || data.get(key).getVersion() > changeData.get(key).getVersion())
        // abort transaction
        getSender().tell(new VoteReponse(Vote.NO, id, vRequest.transactionId), getSelf());
    // If it didn't violate any contraint, thus lock changed key
    for (Integer key : changeData.keySet()) {
      validationLocks.put(key, vRequest.transactionId);
      // store the changes in the sever
      changes.put(key, changeData.get(key).getValue());
    }
    transactionChange.put(vRequest.transactionId, changes);
    mapTransactionCoordinator.put(vRequest.transactionId, getSender());
    // send vote to coordinator
    getSender().tell(new VoteReponse(Vote.YES, id, vRequest.transactionId), getSelf());
    if (r.nextDouble() < TxnSystem.CRASH_PROBABILITY)
      if(this.id==5)
      crash(TxnSystem.CRASH_TIME);
  }

  /**
   * Handle decision response from coordinator
   * 
   * @param decisionResponse
   */
  private void onDecisionResponse(DecisionResponse decisionResponse) {
    Map<Integer, Integer> changes = transactionChange.get(decisionResponse.transactionId);
    // if the decision is Yes, then update storage
    if (decisionResponse.decision == Decision.COMMIT) {
      for (Integer key : changes.keySet()) {
        RowValue record = data.get(key);
        if (record.getValue() != changes.get(key)) {
          RowValue newRecord = new RowValue(record.getVersion() + 1, changes.get(key));

          data.put(key, newRecord);
        }
      }
    }
    // if the decision is not, it means abort, then do nothing
    // both case remove lock and transaction changes
    transactionChange.remove(decisionResponse.transactionId);
    for (Integer key : changes.keySet())
      validationLocks.remove(key);
  }

  @Override
  public Receive createReceive() {
    // TODO Auto-generated method stub
    return receiveBuilder().match(ReadDataMsg.class, this::onReadMsg).match(VoteRequest.class, this::onVoteRequest)
        .match(DecisionResponse.class, this::onDecisionResponse).match(SumTestRequest.class, this::onSumTestRequest)
        .match(Timeout.class, this::onTimeout).build();
  }

/**
 * When crashed, node discard any request in validation and update phase
 */
  @Override
  public Receive crashed() {
    // TODO Auto-generated method stub
    return receiveBuilder().match(ReadDataMsg.class, this::onReadMsg).match(VoteRequest.class, msg -> {
    }).match(DecisionResponse.class, msg -> {
    }).match(SumTestRequest.class, this::onSumTestRequest).match(Timeout.class, this::onTimeout).build();
  }

  /**
   * Recovery after crashing
   */
  @Override
  protected void onRecovery(Recovery msg) {
    // change handle message
    getContext().become(createReceive());
    for (String transactionId : transactionChange.keySet()) {
      print("Recovery. Asking the coordinator.");
      // ask coordinator about the transaction remain in server
      mapTransactionCoordinator.get(transactionId).tell(new DecisionRequest(transactionId), getSelf());
      // set timeout event
      setTimeout(transactionId, TxnSystem.DECISION_TIMEOUT);
    }
  }

  /**
   * Set timeout event
   * 
   * @param transactionId
   * @param time
   */
  void setTimeout(String transactionId, int time) {
    getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
        new Timeout(transactionId), getContext().system().dispatcher(), getSelf());
  }
}
