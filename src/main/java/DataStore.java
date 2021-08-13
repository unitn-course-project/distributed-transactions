import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DataStore extends AbstractActor {
    protected int id;
    protected Map<Integer, Value> data;
    protected Map<Integer, Value> transactionWorkspace;
    protected Map<String, HashMap<Integer, Value>> workspace;
    protected boolean[] validationLock;

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

    public DataStore(int id, Map<Integer, Value> data) {
        super();
        this.id = id;
        this.data = data;
        this.transactionWorkspace = new HashMap<>();
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
        return Props.create(DataStore.class, () -> new DataStore(id, data));
    }

    private void onReadMsg(Message.ReadMsg msg) {
        ActorRef coordinator = getSender();
        int value;
        if(workspace.containsKey(msg.transactionId)){
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            if(modifiedWorkspace.containsKey(msg.key)) {
                value = modifiedWorkspace.get(msg.key).getValue();
            }
            else {
                value = data.get(msg.key).getValue();
            }
        }else{
            Value modifyingValue = data.get(msg.key);
            value = modifyingValue.getValue();
            HashMap<Integer, Value> modifyingWorkspace = new HashMap<>();
            modifyingWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), value));
            workspace.put(msg.transactionId, modifyingWorkspace);
        }
        coordinator.tell(new Message.ReadResultMsg(msg.key, value, msg.transactionId), getSelf());
//        System.out.println("onReadMsg::DataStore "+this.id+" return key="+msg.key+" value="+value);
    }

    private void onWriteMsg(Message.WriteMsg msg) {
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
        } else {
            Value modifyingValue = data.get(msg.key);
            HashMap<Integer, Value> modifyingWorkspace = new HashMap<>();
            modifyingWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), msg.value));
            workspace.put(msg.transactionID, modifyingWorkspace);
        }
//        System.out.println("========= DataStore-" + this.id + "::onWriteMsg =========");
//        printWorkspace();
    }

    private void onVoteRequestMsg(Message.VoteRequestMsg msg) {
        ActorRef coordinator = getSender();

        if (!workspace.containsKey(msg.transactionId)) {
            System.out.println("DataStore-"+this.id+" does not contain "+msg.transactionId);
            coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, true), getSelf());
        } else {
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            for (Integer key : modifiedWorkspace.keySet()) {
                if (validationLock[key % 10]) {
                    System.out.println("DataStore-"+this.id+" "+msg.transactionId+" modify when validating");
                    coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, false), getSelf());
                    return;
                }
                validationLock[key % 10] = true;
            }

            boolean canCommit = true;
            for (Integer key : modifiedWorkspace.keySet()) {
                if (data.get(key).getVersion() != modifiedWorkspace.get(key).getVersion()) {
                    canCommit = false;
                    break;
                }
            }
            System.out.println("DataStore-"+this.id+" "+msg.transactionId+" canCommit="+canCommit);
            coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, canCommit), getSelf());
        }
    }

    private void onDecisionMsg(Message.DecisionMsg msg) {
        if (workspace.containsKey(msg.transactionId)) {
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            if (msg.commit) {
                for (Map.Entry<Integer, Value> element : modifiedWorkspace.entrySet()) {
                    Value updatingValue = data.get(element.getKey());
                    updatingValue.setValue(element.getValue().getValue());
                    updatingValue.setVersion(updatingValue.getVersion() + 1);
                }
            }
            workspace.remove(msg.transactionId);

            for (Integer key : modifiedWorkspace.keySet()) {
                validationLock[key % 10] = false;
            }
        }

        printData(msg.transactionId);
    }

    private void onCheckConsistentMsg(Message.CheckConsistentRequest msg){
        int sum = sumToCheck();
        ActorRef coordinator = getSender();
        coordinator.tell(new Message.CheckConsistentResponse(sum, msg.transactionId), getSelf());
    }

    private void printData(String transactionId){
        StringBuilder printResult = new StringBuilder("========= DataStore-" + this.id + " with " + transactionId + " =========\n");
        for (Map.Entry<Integer, Value> element : data.entrySet()) {
            printResult.append(element.getKey()).append(": ").append(element.getValue().toString()).append("\n");
        }
        printResult.append("======>>>>>> sum = ").append(sumToCheck());
        System.out.println(printResult);
    }

//    private void printWorkspace() {
//        String printResult = "";
//        for (Map.Entry<String, HashMap<Integer, Value>> stringHashMapEntry : workspace.entrySet()) {
//            printResult += (stringHashMapEntry.getKey() + ":\n");
//            HashMap<Integer, Value> tWorkspace = stringHashMapEntry.getValue();
//            for (Map.Entry<Integer, Value> integerValueEntry : tWorkspace.entrySet()) {
//                printResult += ("\t" + integerValueEntry.getKey() + ": " + integerValueEntry.getValue().toString() + "\n");
//            }
//        }
//        System.out.println(printResult);
//    }

    private int sumToCheck(){
        Iterator<Map.Entry<Integer, Value>> it = data.entrySet().iterator();
        int sum = 0;
        while (it.hasNext()) {
            Map.Entry<Integer, Value> element = it.next();
            sum += element.getValue().getValue();
        }
        return sum;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.ReadMsg.class, this::onReadMsg)
                .match(Message.WriteMsg.class, this::onWriteMsg)
                .match(Message.VoteRequestMsg.class, this::onVoteRequestMsg)
                .match(Message.DecisionMsg.class, this::onDecisionMsg)
                .match(Message.CheckConsistentRequest.class, this::onCheckConsistentMsg)
                .build();
    }
}