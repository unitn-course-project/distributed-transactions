import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Coordinator extends AbstractActor {
    protected int id;
    protected HashMap<Integer, ActorRef> mapDataStoreByKey;
    private HashMap<String, ActorRef> mapTransaction2Client;
    private HashMap<ActorRef, String> mapClient2Transaction;
//    private HashMap<String, List<ActorRef>> mapTransaction2DataStores;

    public Coordinator(int id, HashMap<Integer, ActorRef> map) {
        super();
        this.id = id;
        this.mapDataStoreByKey = map;
        mapTransaction2Client = new HashMap<>();
        mapClient2Transaction = new HashMap<>();
//        mapTransaction2DataStores = new HashMap<>();
    }

    static public Props props(int id, HashMap<Integer, ActorRef> map) {
        return Props.create(Coordinator.class, () -> new Coordinator(id, map));
    }

    private void onTxnBeginMsg(Message.TxnBeginMsg msg) {
        ActorRef client = getSender();
        String transactionId = UUID.randomUUID().toString();
        mapTransaction2Client.put(transactionId, client);
        mapClient2Transaction.put(client, transactionId);
        client.tell(new Message.TxnAcceptMsg(), getSelf());
//        System.out.println("Coordinator-" + this.id + " ACCEPT txn begin from " + client);
    }

    private void onReadMsg(Message.ReadMsg msg){
        int dataStoreId = msg.key/10;
        ActorRef dataStore = mapDataStoreByKey.get(dataStoreId);
        msg.transactionId = mapClient2Transaction.get(getSender());
        dataStore.tell(msg, getSelf());
//        System.out.println("Coordinator " + this.id + " forward Read(clientId="+msg.clientId+", key="+msg.key+") to "+dataStoreId);
    }

    private void onReadResultMsg(Message.ReadResultMsg msg){
        ActorRef client = mapTransaction2Client.get(msg.transactionId);
        client.tell(msg, getSelf());
//        System.out.println("Coordinator " + this.id + " forward "+msg.toString());
    }


    private void onWriteMsg(Message.WriteMsg msg){
        ActorRef dataStore = mapDataStoreByKey.get(msg.key/10);
        msg.transactionID = mapClient2Transaction.get(getSender());
        dataStore.tell(msg, getSelf());
    }

    private void onTxnEndMsg(Message.TxnEndMsg msg){
        multicast(new Message.VoteRequestMsg(mapClient2Transaction.get(getSender())));
    }

    void multicast(Serializable m){
        for(ActorRef p: mapDataStoreByKey.values()){
            p.tell(m, getSelf());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(Message.ReadMsg.class, this::onReadMsg)
                .match(Message.ReadResultMsg.class, this::onReadResultMsg)
                .match(Message.WriteMsg.class, this::onWriteMsg)
                .match(Message.TxnEndMsg.class, this::onTxnEndMsg)
                .build();
    }
}