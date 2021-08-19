package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.TxnClient.ReadMsg;
import it.unitn.ds1.TxnClient.ReadResultMsg;
import it.unitn.ds1.TxnClient.TxnAcceptMsg;
import it.unitn.ds1.TxnClient.TxnBeginMsg;
import it.unitn.ds1.TxnClient.TxnEndMsg;
import it.unitn.ds1.TxnClient.TxnResultMsg;
import it.unitn.ds1.TxnClient.WriteMsg;
import it.unitn.ds1.model.PrivateWorkspace;
import it.unitn.ds1.model.RowValue;
import scala.concurrent.duration.Duration;

/**
 * Coordinators manage transaction request and manipulate it among servers
 */
public class TxnCoordinator extends Node {
  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private List<ActorRef> servers;

  // state varibale
  private List<Integer> processingClientIds;
  private Map<Integer, String> currentTransaction;
  private Map<String, Integer> mapCurrentTransaction;
  private Map<String, ActorRef> mapCurrentTransactionActor;
  private Map<String, PrivateWorkspace> processingPrivateWorkspace;

  private Map<String, Set<Integer>> requiredServerVote;
  // history
  private Map<String, Decision> historyTransaction;

  public TxnCoordinator(int id) {
    super(id);
  }

  static public Props props(int id) {
    return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(id));
  }

  @Override
  public void preStart() {
    processingClientIds = new ArrayList<>();
    mapCurrentTransaction = new HashMap<>();
    currentTransaction = new HashMap<>();
    processingPrivateWorkspace = new HashMap<>();
    mapCurrentTransactionActor = new HashMap<>();
    requiredServerVote = new HashMap<>();
    historyTransaction = new HashMap<>();
  }

  /*-- Message classes ------------------------------------------------------ */

  public enum Vote {
    NO, YES
  }

  public enum Decision {
    ABORT, COMMIT
  }

  public static class VoteRequest implements Serializable {
    public final String transactionId;
    public final Map<Integer, RowValue> changes;

    public VoteRequest(String transactionId, Map<Integer, RowValue> changes) {
      this.transactionId = transactionId;
      this.changes = changes;
    }

  }

  public static class VoteReponse implements Serializable {
    public final Vote vote;
    public final Integer clientId;
    public final String transactionId;

    public VoteReponse(Vote vote, Integer clientId, String transactionId) {
      this.vote = vote;
      this.clientId = clientId;
      this.transactionId = transactionId;
    }

  }

  public static class Timeout implements Serializable {
    public final String transactionId;
    public final int serverId;

    public Timeout(String transactionId, int serverId) {
      this.transactionId = transactionId;
      this.serverId = serverId;
    }

  }

  public static class DecisionRequest implements Serializable {
    public final String transactionId;

    public DecisionRequest(String transactionId) {
      this.transactionId = transactionId;
    }

  }

  public static class DecisionResponse implements Serializable {
    public final Decision decision;
    public final String transactionId;

    public DecisionResponse(Decision decision, String transactionId) {
      this.decision = decision;
      this.transactionId = transactionId;
    }

  }

  public static class StartMsg implements Serializable {
    public final List<ActorRef> servers;

    public StartMsg(List<ActorRef> servers) {
      this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
    }
  }

  public static class ReadDataMsg implements Serializable {
    public final String transactionId;
    public final Integer key;

    public ReadDataMsg(String transactionId, Integer key) {
      this.transactionId = transactionId;
      this.key = key;
    }

  }

  public static class ReadDataResultMsg implements Serializable {
    public final String transactionId;
    public final Integer key;
    public final Integer value;
    public final Integer version;

    public ReadDataResultMsg(String transactionId, Integer key, Integer value, Integer version) {
      this.transactionId = transactionId;
      this.key = key;
      this.value = value;
      this.version = version;
    }

  }

  /*-- Message handlers ----------------------------------------------------- */
  private void onStartMsg(StartMsg welcomeMsg) {
    this.servers = welcomeMsg.servers;
    log.info("Coordinator " + id + " recognize " + servers.size() + " servers");
  }

  private void onBeginTxnMsg(TxnBeginMsg txnBeginMsg) {
    this.processingClientIds.add(txnBeginMsg.clientId);
    String transactionId = UUID.randomUUID().toString();
    currentTransaction.put(txnBeginMsg.clientId, transactionId);
    mapCurrentTransaction.put(transactionId, txnBeginMsg.clientId);
    mapCurrentTransactionActor.put(transactionId, getSender());
    // init private workspace
    processingPrivateWorkspace.put(transactionId, new PrivateWorkspace());
    ActorRef sender = getSender();
    sender.tell(new TxnAcceptMsg(), getSelf());
  }

  private void onWriteMsg(WriteMsg writeMsg) {
    Integer clientId = writeMsg.clientId;
    RowValue readValue = null;
    String transactionId = currentTransaction.get(clientId);
    readValue = getDataFromPrivateWorkSpace(writeMsg.key, transactionId);

    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    RowValue rowValue = privateWorkspace.getData().get(writeMsg.key);
    rowValue.setValue(writeMsg.value);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    data.put(writeMsg.key, rowValue);
    privateWorkspace.setData(data);
  }

  private void onReadMsg(ReadMsg readMsg) {
    Integer clientId = readMsg.clientId;
    RowValue readValue = null;
    String transactionId = currentTransaction.get(clientId);
    if (exitsInPrivateWorkSpace(readMsg.key, transactionId)) {
      readValue = getDataFromPrivateWorkSpace(readMsg.key, transactionId);
      getSender().tell(new ReadResultMsg(readMsg.key, readValue.getValue()), getSelf());
    } else {
      mapCurrentTransactionActor.put(transactionId, getSender());
      getDataByKey(readMsg.key, transactionId);
    }

  }

  private void onVoteResponse(VoteReponse vReponse) {
    if (!historyTransaction.containsKey(vReponse.transactionId)) {
      if (vReponse.vote == Vote.YES) {
        Set<Integer> requireVote = requiredServerVote.get(vReponse.transactionId);
        requireVote.remove(vReponse.clientId);
        requiredServerVote.put(vReponse.transactionId, requireVote);
        log.info(requireVote.toString());
        if (requireVote.size() <= 0)
          commitTransaction(vReponse.transactionId);
      } else
        abortTransaction(vReponse.transactionId);
    }
  }

  private void onDecisionRequest(DecisionRequest decisionRequest) {
    if (historyTransaction.containsKey(decisionRequest.transactionId))
      getSender().tell(
          new DecisionResponse(historyTransaction.get(decisionRequest.transactionId), decisionRequest.transactionId),
          getSelf());
    else
      log.error("EROOR anonymous transaction or not decide yet");
  }

  private void onEndTxnMsg(TxnEndMsg endMsg) {
    validationPhase(currentTransaction.get(endMsg.clientId));
  }

  /**
   * Handle result from server for read operator
   * 
   * @param readDataResultMsg
   */
  private void onReadResultMsg(ReadDataResultMsg readDataResultMsg) {
    ActorRef client = mapCurrentTransactionActor.get(readDataResultMsg.transactionId);
    // update private workspace
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(readDataResultMsg.transactionId);
    RowValue rowValue = new RowValue(readDataResultMsg.version, readDataResultMsg.value);
    Map<Integer, RowValue> data = privateWorkspace.getData();

    data.put(readDataResultMsg.key, rowValue);
    privateWorkspace.setData(data);
    // tell client result
    client.tell(new ReadResultMsg(readDataResultMsg.key, readDataResultMsg.value), getSelf());
  }

  private void onTimeout(Timeout timeout) {
    if (!historyTransaction.containsKey(timeout.transactionId))
      if (requiredServerVote.get(timeout.transactionId).contains(timeout.serverId)) {
        abortTransaction(timeout.transactionId);
      }
  }

  @Override
  protected void onRecovery(Recovery msg) {
    getContext().become(createReceive());
    for (String transactionId : mapCurrentTransaction.keySet()) {
      abortTransaction(transactionId);
    }
  }

  private void validationPhase(String transactionId) {

    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    Set<Integer> requiredVote = new HashSet<>();
    Map<Integer, Map<Integer, RowValue>> changesByServer = new HashMap<>();

    for (int key : data.keySet()) {
      Map<Integer, RowValue> dataChanges = null;
      if (changesByServer.containsKey(getServerIdByKey(key)))
        dataChanges = changesByServer.get(getServerIdByKey(key));
      else
        dataChanges = new HashMap();
      dataChanges.put(key, new RowValue(data.get(key).getVersion(), data.get(key).getValue()));
      changesByServer.put(getServerIdByKey(key), dataChanges);
      requiredVote.add(getServerIdByKey(key));
    }

    for (Integer server : requiredVote) {
      servers.get(server).tell(new VoteRequest(transactionId, changesByServer.get(server)), getSelf());
      setTimeout(transactionId, server, TxnSystem.VOTE_TIMEOUT);
    }
    requiredServerVote.put(transactionId, requiredVote);
    // crash(5000);
  }

  /**
   * Get data with key from a proper server
   * 
   * @param key
   * @param clientId
   */

  private void getDataByKey(Integer key, String transactionId) {
    ActorRef server = getServerByKey(key);
    server.tell(new ReadDataMsg(transactionId, key), getSelf());
  }

  /**
   * Get data from private workspace
   * 
   * @param key
   * @param clientId
   * @return
   */
  private RowValue getDataFromPrivateWorkSpace(Integer key, String transactionId) {
    return processingPrivateWorkspace.get(transactionId).getData().get(key);
  }

  /**
   * Check if data has already in private workspace
   * 
   * @param key
   * @param clientId
   * @return
   */
  private boolean exitsInPrivateWorkSpace(Integer key, String transactionId) {
    if (processingPrivateWorkspace.containsKey(transactionId))
      if (processingPrivateWorkspace.get(transactionId).getData().containsKey(key))
        return true;
    return false;
  }

  private void clearPrivateWorkspace(String transactionId) {
    Integer clientId = mapCurrentTransaction.get(transactionId);
    currentTransaction.remove(clientId);
    mapCurrentTransaction.remove(transactionId);
    processingPrivateWorkspace.remove(transactionId);
    processingClientIds.remove(processingClientIds.indexOf(clientId));
    requiredServerVote.remove(transactionId);
    mapCurrentTransactionActor.remove(transactionId);
  }

  private void commitTransaction(String transactionId) {
    historyTransaction.put(transactionId, Decision.COMMIT);
    mapCurrentTransactionActor.get(transactionId).tell(new TxnResultMsg(true), getSelf());
    Set<ActorRef> informingServer = new HashSet<>();
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    for (int key : data.keySet()) {
      informingServer.add(getServerByKey(key));
    }
    for (ActorRef actor : informingServer)
      actor.tell(new DecisionResponse(Decision.COMMIT, transactionId), getSelf());
    clearPrivateWorkspace(transactionId);

  }

  private void abortTransaction(String transactionId) {

    historyTransaction.put(transactionId, Decision.ABORT);
    mapCurrentTransactionActor.get(transactionId).tell(new TxnResultMsg(false), getSelf());
    Set<ActorRef> informingServer = new HashSet<>();
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    for (int key : data.keySet()) {
      informingServer.add(getServerByKey(key));
    }
    for (ActorRef actor : informingServer)
      actor.tell(new DecisionResponse(Decision.ABORT, transactionId), getSelf());
    clearPrivateWorkspace(transactionId);

  }

  private ActorRef getServerByKey(Integer key) {
    return servers.get(key / 10);
  }

  private Integer getServerIdByKey(Integer key) {
    return key / 10;
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(StartMsg.class, this::onStartMsg).match(TxnBeginMsg.class, this::onBeginTxnMsg)
        .match(ReadMsg.class, this::onReadMsg).match(ReadDataResultMsg.class, this::onReadResultMsg)
        .match(WriteMsg.class, this::onWriteMsg).match(TxnEndMsg.class, this::onEndTxnMsg)
        .match(VoteReponse.class, this::onVoteResponse).match(DecisionRequest.class, this::onDecisionRequest)
        .match(Timeout.class, this::onTimeout).match(Recovery.class, this::onRecovery).build(); // why have recovery
                                                                                                // here ?
  }

  void setTimeout(String transactionId, Integer serverId, int time) {
    getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
        new Timeout(transactionId, serverId), getContext().system().dispatcher(), getSelf());
  }
}
