import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.Int;

import java.util.List;
import java.util.Map;

public class DataStore extends AbstractActor {
    protected int id;
    protected Map<Integer, Value> data;

    public static class Value{
        private int version;
        private int value;

        public int getVersion() {
            return version;
        }

        public int getValue() {
            return value;
        }

        public Value(int version, int value){
            this.version = version;
            this.value = value;
        }
    }

    public DataStore(int id, Map<Integer, Value> data){
        super();
        this.id = id;
        this.data = data;
    }

    static public Props props(int id, Map<Integer, Value> data){return Props.create(DataStore.class, () -> new DataStore(id, data));}

    private void onReadMsg(Coordinator.ReadMsg msg){
        ActorRef coordinator = getSender();
        int value = data.get(msg.key).getValue();
        coordinator.tell(new TxnClient.ReadResultMsg(msg.key, value, msg.transactionId), getSelf());
//        System.out.println("onReadMsg::DataStore "+this.id+" return key="+msg.key+" value="+value);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Coordinator.ReadMsg.class, this::onReadMsg)
                .build();
    }
}