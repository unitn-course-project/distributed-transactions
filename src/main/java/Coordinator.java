import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

public class Coordinator extends AbstractActor {
    protected int id;
    protected HashMap<Integer, ActorRef> mapDataStoreByKey;
    private final HashMap<String, ActorRef> mapTransaction2Client;
    private final HashMap<ActorRef, String> mapClient2Transaction;
    private final HashMap<String, HashSet<ActorRef>> yesVoters;
    private final HashMap<String, Boolean> mapTransaction2Decision;
    private final HashMap<String, Integer> checkSum;
    private final HashMap<String, HashSet<ActorRef>> checkSumResponse;

    public Coordinator(int id, HashMap<Integer, ActorRef> map) {
        super();
        this.id = id;
        this.mapDataStoreByKey = map;
        mapTransaction2Client = new HashMap<>();
        mapClient2Transaction = new HashMap<>();
        mapTransaction2Decision = new HashMap<>();
        yesVoters = new HashMap<>();
        checkSum = new HashMap<>();
        checkSumResponse = new HashMap<>();
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
        System.out.println("Coordinator-" + this.id + " ACCEPT txn begin from " + client + " with transactionId=" + transactionId);
    }

    private void onReadMsg(Message.ReadMsg msg) {
        int dataStoreId = msg.key / 10;
        ActorRef dataStore = mapDataStoreByKey.get(dataStoreId);
        msg.transactionId = mapClient2Transaction.get(getSender());
        dataStore.tell(msg, getSelf());
//        System.out.println("Coordinator " + this.id + " forward Read(clientId="+msg.clientId+", key="+msg.key+") to "+dataStoreId);
    }

    private void onReadResultMsg(Message.ReadResultMsg msg) {
        ActorRef client = mapTransaction2Client.get(msg.transactionId);
        client.tell(msg, getSelf());
//        System.out.println("Coordinator " + this.id + " forward "+msg.toString());
    }


    private void onWriteMsg(Message.WriteMsg msg) {
        ActorRef dataStore = mapDataStoreByKey.get(msg.key / 10);
        msg.transactionID = mapClient2Transaction.get(getSender());
        dataStore.tell(msg, getSelf());
    }

    private void onTxnEndMsg(Message.TxnEndMsg msg) {
        if (msg.commit) {
            System.out.println(mapClient2Transaction.get(getSender()) + " beginTxnEndMsg ------->>>>>>>");
            yesVoters.put(mapClient2Transaction.get(getSender()), new HashSet<>());
            multicast(new Message.VoteRequestMsg(mapClient2Transaction.get(getSender())));
        } else {
            String transactionId = mapClient2Transaction.get(getSender());
            fixDecision(transactionId, false);
            multicast(new Message.DecisionMsg(transactionId, mapTransaction2Decision.get(transactionId)));
            System.out.println(transactionId + " abort by itself -> tellDecision2Client ------>>>>>>>>");
            tellDecision2Client(transactionId);
        }
    }

    private void onVoteResponseMsg(Message.VoteResponseMsg msg) {
        //Check if already send decision to client and server
        if (mapTransaction2Client.containsKey(msg.transactionId)) {
            if (msg.commit) {
                HashSet<ActorRef> tranYesVoters = yesVoters.get(msg.transactionId);
                tranYesVoters.add(getSender());
                yesVoters.put(msg.transactionId, tranYesVoters);
                if (allVotedYes(msg.transactionId)) {
                    checkConsistent(msg.transactionId);
                    fixDecision(msg.transactionId, true);
                    multicast(new Message.DecisionMsg(msg.transactionId, mapTransaction2Decision.get(msg.transactionId)));
                    System.out.println(msg.transactionId + " allVoteYes -> tellDecision2Client ------>>>>>>>>");
                    tellDecision2Client(msg.transactionId);
                }
            } else {
                checkConsistent(msg.transactionId);
                fixDecision(msg.transactionId, false);
                multicast(new Message.DecisionMsg(msg.transactionId, mapTransaction2Decision.get(msg.transactionId)));
                System.out.println(msg.transactionId + " existing voteNo -> tellDecision2Client ------>>>>>>>>");
                tellDecision2Client(msg.transactionId);
            }
        }
    }


    //TODO modify implementation
    private void checkConsistent(String transactionId) {
        checkSum.put(transactionId, 0);
        checkSumResponse.put(transactionId, new HashSet<>());
        multicast(new Message.CheckConsistentRequest(transactionId));
    }

    private void onCheckConsistentResponseMsg(Message.CheckConsistentResponse msg) {
        HashSet<ActorRef> voterCheck = checkSumResponse.get(msg.transactionId);
        voterCheck.add(getSender());
        checkSumResponse.put(msg.transactionId, voterCheck);
        int votedSum = checkSum.get(msg.transactionId);
        checkSum.put(msg.transactionId, votedSum + msg.sum);
        if (checkSumResponse.get(msg.transactionId).size() == mapDataStoreByKey.size()) {
            System.out.println(">>>>>>>>>>>>>> " + msg.transactionId + " checkSum = " + checkSum.get(msg.transactionId) + " <<<<<<<<<<<<<<<");
        }
    }

    void multicast(Serializable m) {
        for (ActorRef p : mapDataStoreByKey.values()) {
            p.tell(m, getSelf());
        }
    }

    boolean allVotedYes(String transactionId) {
//        System.out.println("yesVoters size = "+yesVoters.size()+" dataStore size = "+mapDataStoreByKey.size());
        return (yesVoters.get(transactionId).size() == mapDataStoreByKey.size());
    }

    void fixDecision(String transactionId, boolean commit) {
        if (!mapTransaction2Decision.containsKey(transactionId)) {
            mapTransaction2Decision.put(transactionId, commit);
        }
    }

    void tellDecision2Client(String transactionId) {
        try {
            ActorRef client = mapTransaction2Client.get(transactionId);
            boolean txnResult = mapTransaction2Decision.get(transactionId);
            mapTransaction2Decision.remove(transactionId);
            mapClient2Transaction.remove(client);
            mapTransaction2Client.remove(transactionId);
            yesVoters.remove(transactionId);
            client.tell(new Message.TxnResultMsg(txnResult), getSelf());
        } catch (Exception e) {
            e.printStackTrace();
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
                .match(Message.VoteResponseMsg.class, this::onVoteResponseMsg)
                .match(Message.CheckConsistentResponse.class, this::onCheckConsistentResponseMsg)
                .build();
    }
}