package it.unitn.ds1;

import akka.actor.AbstractActor;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class Node extends AbstractActor {
    protected HashMap<String, Boolean> mapTransaction2Decision;
    protected final Random r;

    public Node() {
        mapTransaction2Decision = new HashMap<>();
        r = new Random();
    }

    protected abstract void onRecovery(Message.Recovery msg);

    void fixDecision(String transactionId, boolean commit) {
        mapTransaction2Decision.putIfAbsent(transactionId, commit);
    }

    public void onDecisionRequest(Message.DecisionRequest msg) {
        String transactionId = msg.transactionId;
        if (mapTransaction2Decision.get(transactionId) != null) {
            getSender().tell(new Message.DecisionMsg(transactionId, mapTransaction2Decision.get(transactionId)), getSelf());
        }

    }

    void crash(int recoverIn) {
        getContext().become(crashed());
        System.out.println(getSelf().toString() + " CRASH!!!!!!!");

        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Recovery(),
                getContext().system().dispatcher(), getSelf()
        );
    }

    void setTimeout(int time, String transactionId) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(transactionId),
                getContext().system().dispatcher(), getSelf()
        );
    }

    public Receive crashed() {
        return receiveBuilder()
                .match(Message.Recovery.class, this::onRecovery)
                .matchAny(msg -> {
                })
                .build();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
}