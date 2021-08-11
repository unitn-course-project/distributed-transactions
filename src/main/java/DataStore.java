import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.sun.xml.internal.bind.v2.TODO;

import java.util.*;

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
        this.workspace = new HashMap<String, HashMap<Integer, Value>>();
        constructValidationLock();
    }

    private void constructValidationLock(){
        Set<Integer> listKey = data.keySet();
        validationLock = new boolean[listKey.size()];
        for(Integer k : listKey){
            validationLock[k%10] = false;
        }
    }

    static public Props props(int id, Map<Integer, Value> data) {
        return Props.create(DataStore.class, () -> new DataStore(id, data));
    }

    private void onReadMsg(Message.ReadMsg msg) {
        ActorRef coordinator = getSender();
        int value = data.get(msg.key).getValue();
        coordinator.tell(new Message.ReadResultMsg(msg.key, value, msg.transactionId), getSelf());
//        System.out.println("onReadMsg::DataStore "+this.id+" return key="+msg.key+" value="+value);
    }

    private void onWriteMsg(Message.WriteMsg msg){
        if(workspace.containsKey(msg.transactionID)){
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionID);
            if(modifiedWorkspace.containsKey(msg.key)){
                Value modifiedValue = modifiedWorkspace.get(msg.key);
                modifiedValue.setValue(msg.value);
                modifiedWorkspace.put(msg.key, modifiedValue);
            }else{
                Value modifyingValue = data.get(msg.key);
                modifiedWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), msg.value));
            }
        }else{
            Value modifyingValue = data.get(msg.key);
            HashMap<Integer, Value> modifyingWorkspace = new HashMap<>();
            modifyingWorkspace.put(msg.key, new Value(modifyingValue.getVersion(), msg.value));
            workspace.put(msg.transactionID, modifyingWorkspace);
        }
        System.out.println("========= DataStore-"+this.id+"::onWriteMsg =========");
        printWorkspace();
    }

    private void onVoteRequestMsg(Message.VoteRequestMsg msg){
        ActorRef coordinator = getSender();

        if(!workspace.containsKey(msg.transactionId)) {
            coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, true), getSelf());
        }else{
            HashMap<Integer, Value> modifiedWorkspace = workspace.get(msg.transactionId);
            for(Integer key: modifiedWorkspace.keySet()){
                if(validationLock[key%10]){
                    coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, false), getSelf());
                    return;
                }
                validationLock[key%10] = true;
            }

            boolean canCommit = true;
            for(Integer key: modifiedWorkspace.keySet()){
                if(data.get(key).getVersion() != modifiedWorkspace.get(key).getVersion()){
                    canCommit = false;
                    break;
                }
            }
            coordinator.tell(new Message.VoteResponseMsg(msg.transactionId, canCommit), getSelf());

            //TODO MOVE TO COMMIT PHASE
            for(Integer key: modifiedWorkspace.keySet()){
                validationLock[key%10] = false;
            }
        }
    }

    private void printWorkspace(){
        String printResult = "";
        Iterator it = workspace.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry element = (Map.Entry) it.next();
            printResult += (element.getKey() + ":\n");
            HashMap<Integer, Value> tWorkspace = (HashMap<Integer, Value>) element.getValue();
            Iterator tIter = tWorkspace.entrySet().iterator();
            while (tIter.hasNext()){
                Map.Entry tElement = (Map.Entry) tIter.next();
                printResult += ("\t"+tElement.getKey()+": "+tElement.getValue().toString()+"\n");
            }
        }
        System.out.println(printResult);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.ReadMsg.class, this::onReadMsg)
                .match(Message.WriteMsg.class, this::onWriteMsg)
                .match(Message.VoteRequestMsg.class, this::onVoteRequestMsg)
                .build();
    }
}