import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
    final static int N_COORDINATORS = 3;
    final static int N_CLIENT = 5;
    final static int N_SERVER = 5;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("distributed-transactions");

//        List<ActorRef> dataStores = new ArrayList<>();
        HashMap<Integer, ActorRef> mapDataStoreByKey = new HashMap<>();
        for(int i=0; i<N_SERVER; i++){
            HashMap<Integer, DataStore.Value> data = new HashMap<>();
            for(int j=0; j<10; j++){
                data.put(10*i+j, new DataStore.Value(0, 100));
            }
            ActorRef dataStore = system.actorOf(DataStore.props(i, data));
            mapDataStoreByKey.put(i, dataStore);
//            dataStores.add(dataStore);
        }

        List<ActorRef> coordinators = new ArrayList<>();
        for (int i = 0; i < N_COORDINATORS; i++) {
            coordinators.add(system.actorOf(Coordinator.props(i, mapDataStoreByKey), "coordinator-" + i));
        }

        List<ActorRef> clients = new ArrayList<>();
        for (int i = 0; i < N_CLIENT; i++) {
            clients.add(system.actorOf(TxnClient.props(i), "client-" + i));
        }

        Message.WelcomeMsg start = new Message.WelcomeMsg(N_SERVER*10-1, coordinators);
        for (ActorRef c : clients) {
            c.tell(start, null);
        }
    }
}