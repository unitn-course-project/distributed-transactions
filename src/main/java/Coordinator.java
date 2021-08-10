import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

public class Coordinator extends AbstractActor {
    protected int id;
    protected HashMap<Integer, ActorRef> mapDataStoreByKey;
    private HashMap<String, ActorRef> mapTransaction2Client;

    public Coordinator(int id, HashMap<Integer, ActorRef> map) {
        super();
        this.id = id;
        this.mapDataStoreByKey = map;
        mapTransaction2Client = new HashMap<>();
    }

    static public Props props(int id, HashMap<Integer, ActorRef> map) {
        return Props.create(Coordinator.class, () -> new Coordinator(id, map));
    }

    // message the client sends to a coordinator to begin the TXN
    public static class TxnBeginMsg implements Serializable {
        public final Integer clientId;

        public TxnBeginMsg(int clientId) {
            this.clientId = clientId;
        }
    }

    // READ request from the client to the coordinator
    public static class ReadMsg implements Serializable {
        public final Integer clientId;
        public final Integer key; // the key of the value to read
        public String transactionId;

        public ReadMsg(int clientId, int key) {
            this.clientId = clientId;
            this.key = key;
        }

        @Override
        public String toString() {
            return "ReadMsg{" +
                    "clientId=" + clientId +
                    ", key=" + key +
                    ", transactionId='" + transactionId + '\'' +
                    '}';
        }
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        ActorRef client = getSender();
        client.tell(new TxnClient.TxnAcceptMsg(), getSelf());
//        System.out.println("Coordinator-" + this.id + " ACCEPT txn begin from " + client);
    }

    private void onReadMsg(ReadMsg msg){
        ActorRef client = getSender();
        String transactionId = UUID.randomUUID().toString();
        mapTransaction2Client.put(transactionId, client);
        msg.transactionId = transactionId;
        int dataStoreId = msg.key/10;
        ActorRef dataStore = mapDataStoreByKey.get(dataStoreId);
        dataStore.tell(msg, getSelf());
//        System.out.println("Coordinator " + this.id + " forward Read(clientId="+msg.clientId+", key="+msg.key+") to "+dataStoreId);
    }

    private void onReadResultMsg(TxnClient.ReadResultMsg msg){
        ActorRef client = mapTransaction2Client.get(msg.transactionId);
        client.tell(msg, getSelf());
//        System.out.println("Coordinator " + this.id + " forward "+msg.toString());
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(TxnClient.ReadResultMsg.class, this::onReadResultMsg)
                .build();
    }
}