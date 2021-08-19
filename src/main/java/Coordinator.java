import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

public class Coordinator extends Node {
    protected int id;
    protected HashMap<Integer, ActorRef> mapDataStoreByKey;
    private final HashMap<String, ActorRef> mapTransaction2Client;
    private final HashMap<ActorRef, String> mapClient2Transaction;
    private final HashMap<String, HashSet<ActorRef>> yesVoters;
    private final HashMap<String, Integer> checkSum;
    private final HashMap<String, HashSet<ActorRef>> checkSumResponse;
    private static final double CRASH_PROBABILITY_1 = 0.5;
    private static final double CRASH_PROBABILITY_2 = 0.5;

    private final int VOTE_REQUEST_TIMEOUT = 1000;

    public Coordinator(int id, HashMap<Integer, ActorRef> map) {
        super();
        this.id = id;
        this.mapDataStoreByKey = map;
        mapTransaction2Client = new HashMap<>();
        mapClient2Transaction = new HashMap<>();
        yesVoters = new HashMap<>();
        checkSum = new HashMap<>();
        checkSumResponse = new HashMap<>();
    }

    static public Props props(int id, HashMap<Integer, ActorRef> map) {
        return Props.create(Coordinator.class, () -> new Coordinator(id, map));
    }

    /*
     * At beginning of each transaction
     * Create transactionId and keep map between transactionID and client
     */
    private void onTxnBeginMsg(Message.TxnBeginMsg msg) {
        ActorRef client = getSender();
        String transactionId = UUID.randomUUID().toString();
        mapTransaction2Client.put(transactionId, client);
        mapClient2Transaction.put(client, transactionId);
        client.tell(new Message.TxnAcceptMsg(), getSelf());
    }

    /*
     * Receiving the read message from client
     * Attach client transactionID and forward message to the data-store base on key
     */
    private void onReadMsg(Message.ReadMsg msg) {
        int dataStoreId = msg.key / 10;
        ActorRef dataStore = mapDataStoreByKey.get(dataStoreId);
        msg.transactionId = mapClient2Transaction.get(getSender());
        dataStore.tell(msg, getSelf());
    }

    private void onReadResultMsg(Message.ReadResultMsg msg) {
        ActorRef client = mapTransaction2Client.get(msg.transactionId);
        client.tell(msg, getSelf());
    }

    /*
     * Receiving the writing message from client
     * Attach client transactionID and forward message to the data-store base on key
     */
    private void onWriteMsg(Message.WriteMsg msg) {
        ActorRef dataStore = mapDataStoreByKey.get(msg.key / 10);
        msg.transactionID = mapClient2Transaction.get(getSender());
        dataStore.tell(msg, getSelf());
    }


    private void onTxnEndMsg(Message.TxnEndMsg msg) {
        mapTransaction2Decision.put(mapClient2Transaction.get(getSender()), null);
        if (msg.commit) {
            System.out.println(mapClient2Transaction.get(getSender()) + " beginTxnEndMsg at "+getSelf().toString()+"------->>>>>>>");
            yesVoters.put(mapClient2Transaction.get(getSender()), new HashSet<>());
            if(r.nextDouble() > CRASH_PROBABILITY_1){
                multicast(new Message.VoteRequestMsg(mapClient2Transaction.get(getSender())));
            }else{
                multicastAndCrash(new Message.VoteRequestMsg(mapClient2Transaction.get(getSender())), 3000);
            }
            setTimeout(VOTE_REQUEST_TIMEOUT, mapClient2Transaction.get(getSender()));
        }
        //If abort message, the coordinator will multicast the abort decision to all data-store
        else {
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
                    fixDecision(msg.transactionId, true);
                    if (r.nextDouble() > CRASH_PROBABILITY_2) {
                        decideChange(msg.transactionId);
                    } else {
                        multicastAndCrash(new Message.DecisionMsg(msg.transactionId, mapTransaction2Decision.get(msg.transactionId)), 3000);
                    }
                    System.out.println(msg.transactionId + " allVoteYes -> tellDecision2Client from " + getSelf().toString() + " ------>>>>>>>>");
                }
            }
            else {
                fixDecision(msg.transactionId, false);
                if(r.nextDouble() > CRASH_PROBABILITY_2) {
                    decideChange(msg.transactionId);
                }else{
                    multicastAndCrash(new Message.DecisionMsg(msg.transactionId, mapTransaction2Decision.get(msg.transactionId)), 3000);
                }
                System.out.println(msg.transactionId + " existing voteNo -> tellDecision2Client ------>>>>>>>>");
            }
        }
    }

    private void decideChange(String transactionId){
        multicast(new Message.DecisionMsg(transactionId, mapTransaction2Decision.get(transactionId)));
        tellDecision2Client(transactionId);
        checkConsistent(transactionId);
    }

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

    private void onTimeout(Message.Timeout msg){
        if (mapTransaction2Decision.containsKey(msg.transactionId) && mapTransaction2Decision.get(msg.transactionId) == null) {
            System.out.println(getSelf().toString() + "Timeout " + msg.transactionId + " ABORT");
            fixDecision(msg.transactionId, false);
            decideChange(msg.transactionId);
        }

    }

    void multicast(Serializable m) {
        for (ActorRef p : mapDataStoreByKey.values()) {
            p.tell(m, getSelf());
        }
    }

    void multicastAndCrash(Serializable m, int recoverIn) {
        for (ActorRef p : mapDataStoreByKey.values()) {
            p.tell(m, getSelf());
            crash(recoverIn);
            return;
        }
    }

    boolean allVotedYes(String transactionId) {
//        System.out.println("yesVoters size = "+yesVoters.size()+" dataStore size = "+mapDataStoreByKey.size());
        return (yesVoters.get(transactionId).size() == mapDataStoreByKey.size());
    }

    void tellDecision2Client(String transactionId) {
        try {
            ActorRef client = mapTransaction2Client.get(transactionId);
            boolean txnResult = mapTransaction2Decision.get(transactionId);
            mapTransaction2Decision.remove(transactionId);
            mapClient2Transaction.remove(client);
            mapTransaction2Client.remove(transactionId);
            yesVoters.remove(transactionId);
            client.tell(new Message.TxnResultMsg(txnResult, transactionId), getSelf());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void onRecovery(Message.Recovery msg) {
        getContext().become(createReceive());
        for (String transactionId : mapTransaction2Decision.keySet()) {
            if (mapTransaction2Decision.get(transactionId) == null) {
                System.out.println(getSelf().toString()+"Recovering "+transactionId+", NOT DECIDED");
                fixDecision(transactionId, false);
            }else{
                System.out.println(getSelf().toString()+"Recovering "+transactionId+" decided before crash");
            }
            decideChange(transactionId);
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
                .match(Message.Timeout.class, this::onTimeout)
                .match(Message.DecisionRequest.class, this::onDecisionRequest)
                .build();
    }
}