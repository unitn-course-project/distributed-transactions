import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Message {
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

    // send this message to the client at startup to inform it about the coordinators and the keys
    public static class WelcomeMsg implements Serializable {
        public final Integer maxKey;
        public final List<ActorRef> coordinators;

        public WelcomeMsg(int maxKey, List<ActorRef> coordinators) {
            this.maxKey = maxKey;
            this.coordinators = Collections.unmodifiableList(new ArrayList<>(coordinators));
        }
    }

    // stop the client
    public static class StopMsg implements Serializable {
    }

    // reply from the coordinator receiving TxnBeginMsg
    public static class TxnAcceptMsg implements Serializable {
    }

    // the client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
    public static class TxnAcceptTimeoutMsg implements Serializable {
    }

    // message the client sends to a coordinator to end the TXN;
    // it may ask for commit (with probability COMMIT_PROBABILITY), or abort
    public static class TxnEndMsg implements Serializable {
        public final Integer clientId;
        public final Boolean commit; // if false, the transaction should abort

        public TxnEndMsg(int clientId, boolean commit) {
            this.clientId = clientId;
            this.commit = commit;
        }
    }

    // WRITE request from the client to the coordinator
    public static class WriteMsg implements Serializable {
        public final Integer clientId;
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public String transactionID;

        public WriteMsg(int clientId, int key, int value) {
            this.clientId = clientId;
            this.key = key;
            this.value = value;
        }
    }

    // reply from the coordinator when requested a READ on a given key
    public static class ReadResultMsg implements Serializable {
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public String transactionId;

        public ReadResultMsg(int key, int value, String transactionId) {
            this.key = key;
            this.value = value;
            this.transactionId = transactionId;
        }

        @Override
        public String toString() {
            return "ReadResultMsg{" +
                    "key=" + key +
                    ", value=" + value +
                    ", transactionId='" + transactionId + '\'' +
                    '}';
        }
    }

    // message from the coordinator to the client with the outcome of the TXN
    public static class TxnResultMsg implements Serializable {
        public final Boolean commit; // if false, the transaction was aborted
        public final String transactionId;

        public TxnResultMsg(boolean commit, String transactionId) {
            this.commit = commit;
            this.transactionId = transactionId;
        }
    }

    public static class VoteRequestMsg implements Serializable {
        public String transactionId;

        public VoteRequestMsg(String transactionId) {
            this.transactionId = transactionId;
        }
    }

    public static class VoteResponseMsg implements Serializable {
        public final boolean commit;
        public String transactionId;

        public VoteResponseMsg(String transactionId, boolean commit) {
            this.transactionId = transactionId;
            this.commit = commit;
        }
    }

    public static class DecisionMsg implements Serializable {
        public final boolean commit;
        public String transactionId;

        public DecisionMsg(String transactionId, boolean commit) {
            this.transactionId = transactionId;
            this.commit = commit;
        }
    }

    public static class CheckConsistentRequest implements Serializable {
        public String transactionId;

        public CheckConsistentRequest(String transactionId) {
            this.transactionId = transactionId;
        }
    }

    public static class CheckConsistentResponse implements Serializable {
        public final int sum;
        public final String transactionId;

        public CheckConsistentResponse(int sum, String transactionId) {
            this.sum = sum;
            this.transactionId = transactionId;
        }
    }

    public static class Recovery implements Serializable {
    }

    public static class Timeout implements Serializable {
        public final String transactionId;
        public Timeout(String transactionId){
            this.transactionId = transactionId;
        }
    }

    public static class DecisionRequest implements Serializable {
        public final String transactionId;
        public DecisionRequest(String transactionId){
            this.transactionId = transactionId;
        }
    }

    public static class InitialSetting implements Serializable{
        public final List<ActorRef> coordinators;
        public final List<ActorRef> dataStores;

        public InitialSetting(List<ActorRef> coordinators, List<ActorRef> dataStores){
            this.coordinators = coordinators;
            this.dataStores = dataStores;
        }
    }
}