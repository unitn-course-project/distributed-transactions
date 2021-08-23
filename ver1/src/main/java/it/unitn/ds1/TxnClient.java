package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TxnClient extends AbstractActor {
    private static final double COMMIT_PROBABILITY = 0.9;
    private static final double WRITE_PROBABILITY = 0.5;
    private static final int MIN_TXN_LENGTH = 50;
    private static final int MAX_TXN_LENGTH = 60;
    private static final int RAND_LENGTH_RANGE = MAX_TXN_LENGTH - MIN_TXN_LENGTH + 1;

    private final Integer clientId;
    private List<ActorRef> coordinators;

    // the maximum key associated to items of the store
    private Integer maxKey;

    // keep track of the number of TXNs (attempted, successfully committed)
    private Integer numAttemptedTxn;
    private Integer numCommittedTxn;

    // TXN operation (move some amount from a value to another)
    private Boolean acceptedTxn;
    private ActorRef currentCoordinator;
    private Integer firstKey, secondKey;
    private Integer firstValue, secondValue;
    private Integer numOpTotal;
    private Integer numOpDone;
    private Cancellable acceptTimeout;
    private final Random r;

    /*-- Actor constructor ---------------------------------------------------- */

    public TxnClient(int clientId) {
        this.clientId = clientId;
        this.numAttemptedTxn = 0;
        this.numCommittedTxn = 0;
        this.r = new Random();
    }

    static public Props props(int clientId) {
        return Props.create(TxnClient.class, () -> new TxnClient(clientId));
    }

    /*-- Actor methods -------------------------------------------------------- */

    // start a new TXN: choose a random coordinator, send TxnBeginMsg and set timeout
    void beginTxn() {

        // some delay between transactions from the same client
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        acceptedTxn = false;
        numAttemptedTxn++;

        // contact a random coordinator and begin TXN
        currentCoordinator = coordinators.get(r.nextInt(coordinators.size()));
        currentCoordinator.tell(new Message.TxnBeginMsg(clientId), getSelf());

        // how many operations (taking some amount and adding it somewhere else)?
        int numExtraOp = RAND_LENGTH_RANGE > 0 ? r.nextInt(RAND_LENGTH_RANGE) : 0;
        numOpTotal = MIN_TXN_LENGTH + numExtraOp;
        numOpDone = 0;

        // timeout for confirmation of TXN by the coordinator (sent to self)
        acceptTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(500, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.TxnAcceptTimeoutMsg(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
//        System.out.println("CLIENT " + clientId + " BEGIN");
    }

    // end the current TXN sending TxnEndMsg to the coordinator
    void endTxn() {
        boolean doCommit = r.nextDouble() < COMMIT_PROBABILITY;
        currentCoordinator.tell(new Message.TxnEndMsg(clientId, doCommit), getSelf());
        firstValue = null;
        secondValue = null;
//        System.out.println("CLIENT " + clientId + " END");
    }

    // READ two items (will move some amount from the value of the first to the second)
    void readTwo() {

        // read two different keys
        firstKey = r.nextInt(maxKey + 1);
        int randKeyOffset = 1 + r.nextInt(maxKey - 1);
        secondKey = (firstKey + randKeyOffset) % (maxKey + 1);

        // READ requests
        currentCoordinator.tell(new Message.ReadMsg(clientId, firstKey), getSelf());
        currentCoordinator.tell(new Message.ReadMsg(clientId, secondKey), getSelf());

        // delete the current read values
        firstValue = null;
        secondValue = null;

//        System.out.println("CLIENT " + clientId + " READ #" + numOpDone + " (" + firstKey + "), (" + secondKey + ")");
    }

    // WRITE two items (called with probability WRITE_PROBABILITY after readTwo() values are returned)
    void writeTwo() {

        // take some amount from one value and pass it to the other, then request writes
        Integer amountTaken = 0;
        if (firstValue >= 1) amountTaken = 1 + r.nextInt(firstValue);
        currentCoordinator.tell(new Message.WriteMsg(clientId, firstKey, firstValue - amountTaken), getSelf());
        currentCoordinator.tell(new Message.WriteMsg(clientId, secondKey, secondValue + amountTaken), getSelf());
//        System.out.println("CLIENT " + clientId + " WRITE #" + numOpDone
//                + " taken " + amountTaken
//                + " (" + firstKey + ", " + (firstValue - amountTaken) + "), ("
//                + secondKey + ", " + (secondValue + amountTaken) + ")");
    }

    /*-- it.unitn.ds1.Message handlers ----------------------------------------------------- */

    private void onWelcomeMsg(Message.WelcomeMsg msg) {
        this.coordinators = msg.coordinators;
//        System.out.println(coordinators);
        this.maxKey = msg.maxKey;
        beginTxn();
    }

    private void onStopMsg(Message.StopMsg msg) {
        getContext().stop(getSelf());
    }

    private void onTxnAcceptMsg(Message.TxnAcceptMsg msg) {
        acceptedTxn = true;
        acceptTimeout.cancel();
        readTwo();
    }

    private void onTxnAcceptTimeoutMsg(Message.TxnAcceptTimeoutMsg msg) {
        if (!acceptedTxn) beginTxn();
    }

    private void onReadResultMsg(Message.ReadResultMsg msg) {
//        System.out.println("CLIENT " + clientId + " READ RESULT (" + msg.key + ", " + msg.value + ")");

        // save the read value(s)
        if (msg.key.equals(firstKey)) firstValue = msg.value;
        if (msg.key.equals(secondKey)) secondValue = msg.value;

        boolean opDone = (firstValue != null && secondValue != null);

        // do we only read or also write?
        double writeRandom = r.nextDouble();
        boolean doWrite = writeRandom < WRITE_PROBABILITY;
        if (doWrite && opDone) writeTwo();

        // check if the transaction should end;
        // otherwise, read two again
        if (opDone) numOpDone++;
        if (numOpDone >= numOpTotal) {
            endTxn();
        } else if (opDone) {
            readTwo();
        }
    }

    private void onTxnResultMsg(Message.TxnResultMsg msg) {
        if (msg.commit) {
            numCommittedTxn++;
            System.out.println("Transaction " + msg.transactionId + " COMMIT OK (" + numCommittedTxn + "/" + numAttemptedTxn + ")");
        } else {
            System.out.println("Transaction " + msg.transactionId + " COMMIT FAIL (" + (numAttemptedTxn - numCommittedTxn) + "/" + numAttemptedTxn + ")");
        }
        if(numAttemptedTxn < 10){
            beginTxn();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.WelcomeMsg.class, this::onWelcomeMsg)
                .match(Message.TxnAcceptMsg.class, this::onTxnAcceptMsg)
                .match(Message.TxnAcceptTimeoutMsg.class, this::onTxnAcceptTimeoutMsg)
                .match(Message.ReadResultMsg.class, this::onReadResultMsg)
                .match(Message.TxnResultMsg.class, this::onTxnResultMsg)
                .match(Message.StopMsg.class, this::onStopMsg)
                .build();
    }
}
