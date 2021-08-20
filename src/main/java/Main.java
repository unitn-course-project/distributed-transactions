import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
    final static int N_COORDINATORS = 5;
    final static int N_CLIENT = 20;
    final static int N_SERVER = 10;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("distributed-transactions");

        //Construct servers
        HashMap<Integer, ActorRef> mapServerByKey = new HashMap<>();
        List<ActorRef> servers = new ArrayList<>();
        for(int i=0; i<N_SERVER; i++){
            HashMap<Integer, Server.Value> data = new HashMap<>();
            for(int j=0; j<10; j++){
                data.put(10*i+j, new Server.Value(0, 100));
            }
            ActorRef server = system.actorOf(Server.props(i, data), "server-"+i);
            mapServerByKey.put(i, server);
            servers.add(server);
        }

        //Construct the coordinators
        List<ActorRef> coordinators = new ArrayList<>();
        for (int i = 0; i < N_COORDINATORS; i++) {
            coordinators.add(system.actorOf(Coordinator.props(i, mapServerByKey), "coordinator-" + i));
        }

        //For each server initialize coordinator and other data-store
        for(ActorRef store : servers){
            store.tell(new Message.InitialSetting(coordinators, servers), null);
        }

        //Construct client
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