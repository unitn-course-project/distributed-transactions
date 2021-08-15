package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.TxnClient.ReadMsg;
import it.unitn.ds1.TxnClient.ReadResultMsg;
import it.unitn.ds1.TxnClient.TxnAcceptMsg;
import it.unitn.ds1.TxnClient.TxnBeginMsg;
import it.unitn.ds1.model.PrivateWorkspace;
import it.unitn.ds1.model.RowValue;

/**
 * Coordinators manage transaction request and manipulate it among servers
 */
public class TxnCoordinator extends AbstractActor {
  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private int id;
  private List<ActorRef> servers;

  // state varibale
  private List<Integer> processingClientIds;
  private Map<Integer, String> currentTransaction;
  private Map<String, Integer> mapCurrentTransaction;
  private Map<String, PrivateWorkspace> processingPrivateWorkspace;

  private Map<String, ActorRef> waitList;

  public TxnCoordinator(int id) {
    this.id = id;
  }

  static public Props props(int id) {
    return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(id));
  }

  @Override
  public void preStart() {
    processingClientIds = new ArrayList<>();
    mapCurrentTransaction= new HashMap<>();
    currentTransaction = new HashMap<>();
    processingPrivateWorkspace = new HashMap<>();
    waitList = new HashMap<>();
  }

  /*-- Message classes ------------------------------------------------------ */
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
    //init private workspace
    processingPrivateWorkspace.put(transactionId, new PrivateWorkspace());
    ActorRef sender = getSender();
    sender.tell(new TxnAcceptMsg(), getSelf());
  }

  private void onReadMsg(ReadMsg readMsg) {
    Integer clientId = readMsg.clientId;
    RowValue readValue = null;
    String transactionId= currentTransaction.get(clientId);
    if (exitsInPrivateWorkSpace(readMsg.key, transactionId)) {
      readValue = getDataFromPrivateWorkSpace(readMsg.key, transactionId);
      getSender().tell(new ReadResultMsg(readMsg.key, readValue.getValue()), getSelf());
    } else {
      getDataByKey(readMsg.key, transactionId);
    }

  }

  private void onReadResultMsg(ReadDataResultMsg readDataResultMsg) {
    ActorRef client = waitList.get(readDataResultMsg.transactionId);
    // update private workspace
    PrivateWorkspace privateWorkspace = processingPrivateWorkspace.get(readDataResultMsg.transactionId);
    RowValue rowValue = new RowValue(readDataResultMsg.version, readDataResultMsg.value);
    Map<Integer,RowValue> data=privateWorkspace.getData();

    data.put(readDataResultMsg.key, rowValue);
    privateWorkspace.setData(data);
    // tell client result
    client.tell(new ReadResultMsg(readDataResultMsg.key, readDataResultMsg.value), getSelf());
  }

  /**
   * Get data with key from a proper server
   * 
   * @param key
   * @param clientId
   */

  private void getDataByKey(Integer key, String transactionId) {
    ActorRef server = getServerByKey(key);
    waitList.put(transactionId, getSender());
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

  private ActorRef getServerByKey(Integer key) {
    return servers.get(key / 10);
  }

  @Override
  public Receive createReceive() {
    // TODO Auto-generated method stub
    return receiveBuilder().match(StartMsg.class, this::onStartMsg).match(TxnBeginMsg.class, this::onBeginTxnMsg)
        .match(ReadMsg.class, this::onReadMsg).match(ReadDataResultMsg.class, this::onReadResultMsg).build();
  }

}
