import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;

public class Coordinator extends AbstractActor {
    protected int id;

    public Coordinator(int id) {
        super();
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
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

        public ReadMsg(int clientId, int key) {
            this.clientId = clientId;
            this.key = key;
        }
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        ActorRef client = getSender();
        client.tell(new TxnClient.TxnAcceptMsg(), getSelf());
        System.out.println("Coordinator-" + this.id + " ACCEPT txn begin from " + client);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .build();
    }
}