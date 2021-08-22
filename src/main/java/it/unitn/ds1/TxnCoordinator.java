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

  /**
   * Process when get a begin message from client
   * 
   * @param txnBeginMsg
   */
  private void onBeginTxnMsg(TxnBeginMsg txnBeginMsg) {
    // store client Id
    this.processingClientIds.add(txnBeginMsg.clientId);
    // create transaction id
    String transactionId = UUID.randomUUID().toString();
    // store useful information about client and associated with transaction
    currentTransaction.put(txnBeginMsg.clientId, transactionId);
    mapCurrentTransaction.put(transactionId, txnBeginMsg.clientId);
    mapCurrentTransactionActor.put(transactionId, getSender());
    // init private workspace
    processingPrivateWorkspace.put(transactionId, new PrivateWorkspace());
    ActorRef sender = getSender();
    // accept transaction
    sender.tell(new TxnAcceptMsg(), getSelf());
  }

  /**
   * Process when recive a write message
   * 
   * @param writeMsg
   */
  private void onWriteMsg(WriteMsg writeMsg) {
    Integer clientId = writeMsg.clientId;
    String transactionId = currentTransaction.get(clientId);
    // RowValue readValue = getDataFromPrivateWorkSpace(writeMsg.key,
    // transactionId);
    // Save write operator to private workspace
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    RowValue rowValue = privateWorkspace.getData().get(writeMsg.key);
    rowValue.setValue(writeMsg.value);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    data.put(writeMsg.key, rowValue);
    privateWorkspace.setData(data);
  }

  /**
   * Handle a read request
   * 
   * @param readMsg
   */
  private void onReadMsg(ReadMsg readMsg) {
    Integer clientId = readMsg.clientId;
    RowValue readValue = null;
    String transactionId = currentTransaction.get(clientId);
    // Check if key is in private workspace or not
    if (exitsInPrivateWorkSpace(readMsg.key, transactionId)) {
      // if data has already in private workspace, the send it to client
      readValue = getDataFromPrivateWorkSpace(readMsg.key, transactionId);
      getSender().tell(new ReadResultMsg(readMsg.key, readValue.getValue()), getSelf());
    } else {
      // read data from server by key
      mapCurrentTransactionActor.put(transactionId, getSender());
      getDataByKey(readMsg.key, transactionId);
    }

  }

  /**
   * Process VoteResponse message
   * 
   * @param vReponse
   */
  private void onVoteResponse(VoteReponse vReponse) {
    // check if coordinator decided or not
    if (!historyTransaction.containsKey(vReponse.transactionId)) {
      if (vReponse.vote == Vote.YES) {
        // check number of vote
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

  /**
   * Handle decision request
   * 
   * @param decisionRequest
   */
  private void onDecisionRequest(DecisionRequest decisionRequest) {
    if (historyTransaction.containsKey(decisionRequest.transactionId))
      // return decision store in historied transaction
      getSender().tell(
          new DecisionResponse(historyTransaction.get(decisionRequest.transactionId), decisionRequest.transactionId),
          getSelf());
    else
      // if the transaction has not decided yet, log error to screen
      log.error("EROOR anonymous transaction or not decide yet");
  }

  /**
   * Handle EndTxnMsg start validation phase
   * 
   * @param endMsg
   */
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

  /**
   * Handle timeout message
   * 
   * @param timeout
   */
  private void onTimeout(Timeout timeout) {
    // check if transaction is decided or not
    if (!historyTransaction.containsKey(timeout.transactionId))
      // if transaction has not decided yet, abort it
      if (requiredServerVote.get(timeout.transactionId).contains(timeout.serverId)) {
        abortTransaction(timeout.transactionId);
      }
  }

  /**
   * handle recovery
   */
  @Override
  protected void onRecovery(Recovery msg) {
    // update handle message
    getContext().become(createReceive());
    // abort any transaction has not decided and lost voteresponse
    for (String transactionId : mapCurrentTransaction.keySet()) {
      abortTransaction(transactionId);
    }
  }

  /**
   * process validation
   * 
   * @param transactionId
   */
  private void validationPhase(String transactionId) {
    // get data from private workspace
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    Set<Integer> requiredVote = new HashSet<>();
    Map<Integer, Map<Integer, RowValue>> changesByServer = new HashMap<>();
    // organize changes by server
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
    // ask server validate changes and set timeout event
    for (Integer server : requiredVote) {
      servers.get(server).tell(new VoteRequest(transactionId, changesByServer.get(server)), getSelf());
      setTimeout(transactionId, server, TxnSystem.VOTE_TIMEOUT);
    }
    // mark required server
    requiredServerVote.put(transactionId, requiredVote);
    // simulate crash
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

  /**
   * Clear private workspace after using
   * 
   * @param transactionId
   */
  private void clearPrivateWorkspace(String transactionId) {
    Integer clientId = mapCurrentTransaction.get(transactionId);
    currentTransaction.remove(clientId);
    mapCurrentTransaction.remove(transactionId);
    processingPrivateWorkspace.remove(transactionId);
    processingClientIds.remove(processingClientIds.indexOf(clientId));
    requiredServerVote.remove(transactionId);
    mapCurrentTransactionActor.remove(transactionId);
  }

  /**
   * Update phase commit transactiokn
   * 
   * @param transactionId
   */
  private void commitTransaction(String transactionId) {
    // record decision to history transaction map
    historyTransaction.put(transactionId, Decision.COMMIT);
    mapCurrentTransactionActor.get(transactionId).tell(new TxnResultMsg(true), getSelf());
    Set<ActorRef> informingServer = new HashSet<>();
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    for (int key : data.keySet()) {
      informingServer.add(getServerByKey(key));
    }
    // inform related server
    for (ActorRef actor : informingServer)
      actor.tell(new DecisionResponse(Decision.COMMIT, transactionId), getSelf());
    // clear private workspace
    clearPrivateWorkspace(transactionId);
  }

  /**
   * Update phase arbort transaction
   * 
   * @param transactionId
   */
  private void abortTransaction(String transactionId) {
    // record decision to history transaction map
    historyTransaction.put(transactionId, Decision.ABORT);
    mapCurrentTransactionActor.get(transactionId).tell(new TxnResultMsg(false), getSelf());
    Set<ActorRef> informingServer = new HashSet<>();
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(transactionId);
    Map<Integer, RowValue> data = privateWorkspace.getData();
    // inform decision to related server
    for (int key : data.keySet()) {
      informingServer.add(getServerByKey(key));
    }
    for (ActorRef actor : informingServer)
      actor.tell(new DecisionResponse(Decision.ABORT, transactionId), getSelf());
    // clear private workspace
    clearPrivateWorkspace(transactionId);

  }

  /**
   * Get Server ActorRef by storage key
   * 
   * @param key
   * @return
   */
  private ActorRef getServerByKey(Integer key) {
    return servers.get(key / 10);
  }

  /**
   * Get server id by storage key
   * 
   * @param key
   * @return
   */
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

  /**
   * Create time out event (message)
   * 
   * @param transactionId
   * @param serverId
   * @param time
   */
  void setTimeout(String transactionId, Integer serverId, int time) {
    getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
        new Timeout(transactionId, serverId), getContext().system().dispatcher(), getSelf());
  }
}
