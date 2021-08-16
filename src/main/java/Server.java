import akka.actor.AbstractActor;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class Server extends AbstractActor {
    protected HashMap<String, Boolean> mapTransaction2Decision;
    protected static final double CRASH_PROBABILITY_1 = 0.5;
    protected static final double CRASH_PROBABILITY_2 = 0;
    protected final Random r;

    public Server(){
        mapTransaction2Decision = new HashMap<>();
        r = new Random();
    }

    protected abstract void onRecovery(Message.Recovery msg);

    void fixDecision(String transactionId, boolean commit) {
        mapTransaction2Decision.putIfAbsent(transactionId, commit);
    }

    void crash(int recoverIn){
        getContext().become(crashed());
        System.out.println(getSelf().toString()+" CRASH!!!!!!!");

        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Recovery(),
                getContext().system().dispatcher(), getSelf()
        );
    }

    public Receive crashed(){
        return receiveBuilder()
                .match(Message.Recovery.class, this::onRecovery)
                .matchAny(msg -> {})
                .build();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
}