package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.*;

public class Server extends Node {
    protected int id;
    protected Map<Integer, Value> data;
    protected Map<String, HashMap<Integer, Value>> workspace;
    protected boolean[] validationLock;
    protected List<ActorRef> coordinators;
    protected List<ActorRef> servers;
    private final int DECISION_TIMEOUT = 1000;
    private static final double CRASH_PROBABILITY = 0.5;

    public static class Value {
        private int version;
        private int value;

        public int getVersion() {
            return version;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public Value(int version, int value) {
            this.version = version;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Value{" +
                    "version=" + version +
                    ", value=" + value +
                    '}';
        }
    }

    public Server(int id, Map<Integer, Value> data) {
        super();
        this.id = id;
        this.data = data;
        this.workspace = new HashMap<>();
        constructValidationLock();
    }

    private void constructValidationLock() {
        Set<Integer> listKey = data.keySet();
        validationLock = new boolean[listKey.size()];
        for (Integer k : listKey) {
            validationLock[k % 10] = false;
        }
    }

    static public Props props(int id, Map<Integer, Value> data) {
        return Props.create(Server.class, () -> new Server(id, data));
    }

    private void initialSetting(Message.InitialSetting msg){
        this.coordinators = msg.coordinators;
        this.servers = msg.servers;
    }

    /*
     * Receiving read message
     */
    private void onReadMsg(Message.ReadMsg msg) {
        ActorRef coordinator = getSender();
        int value;

        // Check in private workspace that if this transaction are modifying value, return modifying value
        if (workspace.containsKey(msg.transactionId)) {
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            if (modifiedWorkspace.containsKey(msg.key)) {
                value = modifiedWorkspace.get(msg.key).getValue();
            } else {
                value = data.get(msg.key).getValue();
            }
        }
        // Construct new workspace which is a copy of result in actual store
        else {
            Value modifyingValue = data.get(msg.key);
            value = modifyingValue.getValue();
            HashMap<Integer, Value> modifyingWorkspace = new HashMap<>();
            modifyingWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), value));
            workspace.put(msg.transactionId, modifyingWorkspace);
        }
        coordinator.tell(new Message.ReadResultMsg(msg.key, value, msg.transactionId), getSelf());
    }

    /*
     * Receiving write message
     */
    private void onWriteMsg(Message.WriteMsg msg) {
        // Check in private workspace that if this transaction are modifying value, modifying value in private workspace
        if (workspace.containsKey(msg.transactionID)) {
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionID);
            if (modifiedWorkspace.containsKey(msg.key)) {
                Value modifiedValue = modifiedWorkspace.get(msg.key);
                modifiedValue.setValue(msg.value);
                modifiedWorkspace.put(msg.key, modifiedValue);
            } else {
                Value modifyingValue = data.get(msg.key);
                modifiedWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), msg.value));
            }
        }
        // Construct new workspace which is a copy of result in actual store and modify the constructed copy
        else {
            Value modifyingValue = data.get(msg.key);
            HashMap<Integer, Value> modifyingWorkspace = new HashMap<>();
            modifyingWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), msg.value));
            workspace.put(msg.transactionID, modifyingWorkspace);
        }
    }

    private void onVoteRequestMsg(Message.VoteRequestMsg msg) {
        ActorRef coordinator = getSender();
        mapTransaction2Decision.put(msg.transactionId, null);

        // If does not contain any modification of sending transactionId, this server can commit
        if (!workspace.containsKey(msg.transactionId)) {
            coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, true), getSelf());
        }
        //Validation-phase
        else {
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            // Check each modifying data-item is in validation phase of other transaction
            for (Integer key : modifiedWorkspace.keySet()) {
                if (validationLock[key % 10]) {
                    fixDecision(msg.transactionId, false);
                    coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, false), getSelf());
                    return;
                }
                validationLock[key % 10] = true;
            }

            //Check if there is exist data-item has the different version from stored data-item
            boolean canCommit = true;
            for (Integer key : modifiedWorkspace.keySet()) {
                if (data.get(key).getVersion() != modifiedWorkspace.get(key).getVersion()) {
                    canCommit = false;
                    break;
                }
            }

            if(!canCommit)
                fixDecision(msg.transactionId, false);
            coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, canCommit), getSelf());
            setTimeout(DECISION_TIMEOUT, msg.transactionId);
            if(r.nextDouble() < CRASH_PROBABILITY){
                crash(3000);
            }
        }
    }

    private void onDecisionMsg(Message.DecisionMsg msg) {
        System.out.println("onDecisionMsg::" + msg.transactionId);
        if (workspace.containsKey(msg.transactionId)) {
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            if (msg.commit) {
                System.out.println("onDecisionMsg::" + msg.transactionId + " apply change");
                for (Map.Entry<Integer, Value> element : modifiedWorkspace.entrySet()) {
                    Value updatingValue = data.get(element.getKey());
                    updatingValue.setValue(element.getValue().getValue());
                    updatingValue.setVersion(updatingValue.getVersion() + 1);
                }
                fixDecision(msg.transactionId, true);
            }else
                fixDecision(msg.transactionId, false);

            workspace.remove(msg.transactionId);

            for (Integer key : modifiedWorkspace.keySet()) {
                validationLock[key % 10] = false;
            }
        }

        printData(msg.transactionId);
    }

    private void onCheckConsistentMsg(Message.CheckConsistentRequest msg) {
        int sum = sumToCheck();
        ActorRef coordinator = getSender();
        coordinator.tell(new Message.CheckConsistentResponse(sum, msg.transactionId), getSelf());
    }

    private void printData(String transactionId) {
        StringBuilder printResult = new StringBuilder("========= it.unitn.ds1.Server-" + this.id + " with " + transactionId + " =========\n");
        for (Map.Entry<Integer, Value> element : data.entrySet()) {
            printResult.append(element.getKey()).append(": ").append(element.getValue().toString()).append("\n");
        }
        printResult.append("======>>>>>> sum = ").append(sumToCheck());
        System.out.println(printResult);
    }

    private int sumToCheck() {
        Iterator<Map.Entry<Integer, Value>> it = data.entrySet().iterator();
        int sum = 0;
        while (it.hasNext()) {
            Map.Entry<Integer, Value> element = it.next();
            sum += element.getValue().getValue();
        }
        return sum;
    }

    private void onTimeout(Message.Timeout msg){
        if (mapTransaction2Decision.get(msg.transactionId) == null) {
            System.out.println(getSelf().toString() + "Timeout " + msg.transactionId + " Asking around");
            for (ActorRef p : servers)
                if(p != getSelf())
                    p.tell(new Message.DecisionRequest(msg.transactionId), getSelf());

            for (ActorRef p : coordinators)
                p.tell(new Message.DecisionRequest(msg.transactionId), getSelf());
            setTimeout(DECISION_TIMEOUT, msg.transactionId);
        }
    }

    @Override
    protected void onRecovery(Message.Recovery msg) {
        getContext().become(createReceive());
        for (String transactionId : mapTransaction2Decision.keySet()) {
            if (mapTransaction2Decision.get(transactionId) == null) {
                System.out.println(getSelf() + " Recovery asking coordinator for transaction " + transactionId);
                for (ActorRef p : coordinators) {
                    p.tell(new Message.DecisionRequest(transactionId), getSelf());
                    setTimeout(DECISION_TIMEOUT, transactionId);
                }
            }
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.ReadMsg.class, this::onReadMsg)
                .match(Message.WriteMsg.class, this::onWriteMsg)
                .match(Message.VoteRequestMsg.class, this::onVoteRequestMsg)
                .match(Message.DecisionMsg.class, this::onDecisionMsg)
                .match(Message.CheckConsistentRequest.class, this::onCheckConsistentMsg)
                .match(Message.InitialSetting.class, this::initialSetting)
                .match(Message.Timeout.class, this::onTimeout)
                .match(Message.DecisionRequest.class, this::onDecisionRequest)
                .build();
    }
}