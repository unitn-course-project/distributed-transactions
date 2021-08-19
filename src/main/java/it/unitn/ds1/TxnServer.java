package it.unitn.ds1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
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

public class TxnServer extends Node {
  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

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
    for (int i = 10 * id; i < 10 * (id + 1); i++) {
      data.put(i, new RowValue(0, 100));
    }
    validationLocks = new HashMap<>();
    transactionChange = new HashMap<>();
    mapTransactionCoordinator = new HashMap<>();
  }
  /*-- Message classes ------------------------------------------------------ */

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
  private void onReadMsg(ReadDataMsg readMsg) {
    RowValue readValue = data.get(readMsg.key);
    getSender().tell(
        new ReadDataResultMsg(readMsg.transactionId, readMsg.key, readValue.getValue(), readValue.getVersion()),
        getSelf());
  }

  private void onTimeout(Timeout timeout) {
    if (transactionChange.containsKey(timeout.transactionId)) {
      mapTransactionCoordinator.get(timeout.transactionId).tell(new DecisionRequest(timeout.transactionId), getSelf());
      setTimeout(timeout.transactionId, TxnSystem.DECISION_TIMEOUT);
    }
  }

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
      // sumTestRequest.testId + " sum =" + sum+"\n");
      // writer.write(sumTestRequest.testId + ", " + sum+"\n");
      writer.write(sum + "\n");

      writer.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void onVoteRequest(VoteRequest vRequest) {
    // if (this.id==0) {
    // crash(5000);
    // return;
    // }
    Map<Integer, RowValue> changeData = vRequest.changes;
    Map<Integer, Integer> changes = new HashMap<>();
    for (Integer key : changeData.keySet())
      if (validationLocks.containsKey(key) || data.get(key).getVersion() > changeData.get(key).getVersion())
        // TODO process when abort
        getSender().tell(new VoteReponse(Vote.NO, id, vRequest.transactionId), getSelf());

    for (Integer key : changeData.keySet()) {
      validationLocks.put(key, vRequest.transactionId);
      changes.put(key, changeData.get(key).getValue());
    }
    transactionChange.put(vRequest.transactionId, changes);
    mapTransactionCoordinator.put(vRequest.transactionId, getSender());
    getSender().tell(new VoteReponse(Vote.YES, id, vRequest.transactionId), getSelf());
  }

  private void onDecisionResponse(DecisionResponse decisionResponse) {
    Map<Integer, Integer> changes = transactionChange.get(decisionResponse.transactionId);
    if (decisionResponse.decision == Decision.COMMIT) {
      for (Integer key : changes.keySet()) {
        RowValue record = data.get(key);
        if (record.getValue() != changes.get(key)) {
          RowValue newRecord = new RowValue(record.getVersion() + 1, changes.get(key));

          data.put(key, newRecord);
        }
      }
    }
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

  @Override
  protected void onRecovery(Recovery msg) {
    getContext().become(createReceive());
    for (String transactionId : transactionChange.keySet()) {
      print("Recovery. Asking the coordinator.");
      mapTransactionCoordinator.get(transactionId).tell(new DecisionRequest(transactionId), getSelf());
      setTimeout(transactionId, TxnSystem.DECISION_TIMEOUT);
    }
  }

  void setTimeout(String transactionId, int time) {
    getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
        new Timeout(transactionId), getContext().system().dispatcher(), getSelf());
  }
}
