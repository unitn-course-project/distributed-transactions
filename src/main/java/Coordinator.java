import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Coordinator extends AbstractActor {
    protected int id;
    protected HashMap<Integer, ActorRef> mapDataStoreByKey;
    private HashMap<String, ActorRef> mapTransaction2Client;
    private HashMap<ActorRef, String> mapClient2Transaction;
    private HashMap<String, HashSet<ActorRef>> yesVoters;
    private HashMap<String, Boolean> mapTransaction2Decision;
    private int checkSum;
    private Set<ActorRef> checkSumResponse;

    public Coordinator(int id, HashMap<Integer, ActorRef> map) {
        super();
        this.id = id;
        this.mapDataStoreByKey = map;
        mapTransaction2Client = new HashMap<>();
        mapClient2Transaction = new HashMap<>();
        mapTransaction2Decision = new HashMap<>();
        yesVoters = new HashMap<>();
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
//        System.out.println(mapTransaction2Client.toString());
//        System.out.println(mapClient2Transaction.toString());
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
        System.out.println(mapClient2Transaction.get(getSender()) + " beginTxnEndMsg ------->>>>>>>");
        yesVoters.put(mapClient2Transaction.get(getSender()), new HashSet<>());
        multicast(new Message.VoteRequestMsg(mapClient2Transaction.get(getSender())));
    }

    private void onVoteResponseMsg(Message.VoteResponseMsg msg) {
        //Check if already send decision to client and server
        if (mapTransaction2Client.containsKey(msg.transactionId)) {
            if (msg.commit) {
                HashSet<ActorRef> tranYesVoters = yesVoters.get(msg.transactionId);
                tranYesVoters.add(getSender());
                yesVoters.put(msg.transactionId, tranYesVoters);
                if (allVotedYes(msg.transactionId)) {
                    fixDecision(msg.transactionId, true);
                    multicast(new Message.DecisionMsg(msg.transactionId, mapTransaction2Decision.get(msg.transactionId)));
                    System.out.println(msg.transactionId + " allVoteYes -> tellDecision2Client ------>>>>>>>>");
                    tellDecision2Client(msg.transactionId);
                }
            } else {
                fixDecision(msg.transactionId, false);
                multicast(new Message.DecisionMsg(msg.transactionId, mapTransaction2Decision.get(msg.transactionId)));
                System.out.println(msg.transactionId + " existing voteNo -> tellDecision2Client ------>>>>>>>>");
                tellDecision2Client(msg.transactionId);
            }
        }
    }

    //TODO modify implementation
    private void onCheckConsistentMsg(Message.CheckConsistentRequest msg){
        multicast(msg);
        checkSum = 0;
        checkSumResponse = new HashSet<>();
    }

    private void onCheckConsistentResponseMsg(Message.CheckConsistentResponse msg){
        checkSumResponse.add(getSender());
        checkSum += msg.sum;
        if(checkSumResponse.size() == mapDataStoreByKey.size()){
            System.out.println(">>>>>>>>>>>>>> checkSum = "+checkSum+" <<<<<<<<<<<<<<<");
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
                .match(Message.CheckConsistentRequest.class, this::onCheckConsistentMsg)
                .match(Message.CheckConsistentResponse.class, this::onCheckConsistentResponseMsg)
                .build();
    }
}