import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.List;

public class Main {
    final static int N_COORDINATORS = 3;
    final static int N_CLIENT = 5;
    final static int N_SERVER = 3;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("distributed-transactions");

        List<ActorRef> coordinators = new ArrayList<>();
        for (int i = 0; i < N_COORDINATORS; i++) {
            coordinators.add(system.actorOf(Coordinator.props(i), "coordinator-" + i));
        }

        List<ActorRef> clients = new ArrayList<>();
        for (int i = 0; i < N_CLIENT; i++) {
            clients.add(system.actorOf(TxnClient.props(i), "client-" + i));
        }

        TxnClient.WelcomeMsg start = new TxnClient.WelcomeMsg(100, coordinators);
        for (ActorRef c : clients) {
            c.tell(start, null);
        }
    }
}