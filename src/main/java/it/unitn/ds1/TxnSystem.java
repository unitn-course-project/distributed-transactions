package it.unitn.ds1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.TxnClient.WelcomeMsg;
import it.unitn.ds1.TxnCoordinator.StartMsg;
import it.unitn.ds1.TxnServer.SumTestRequest;
/**
 *  - Start simulation
 *  - Initial actors and control simulation
 */
public class TxnSystem {
  // Number of Servers
  final static int N_SERVERS = 10;
  // Number of Cordinators
  final static int N_CORDINATORS = 10;
  // Using N_CLIENTS= 10 for correctness simulation and N_CLIENTS =1 for crash simulation
  //final static int N_CLIENTS = 10;
  final static int N_CLIENTS = 1;
  // LOG File for checking correctness
  public final static String LOG_SUM_FILENAME = "sum.txt";
  public final static int DECISION_TIMEOUT= 2000;
  public final static int VOTE_TIMEOUT= 1000;

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("tnxsystem");

    // Create servers and put them to a list
    List<ActorRef> servers = new ArrayList<>();
    for (int i = 0; i < N_SERVERS; i++) {
      servers.add(system.actorOf(TxnServer.props(i), "server" + i));
    }

    // Create coordinators and put them to a list
    List<ActorRef> coordinators = new ArrayList<>();
    for (int i = 0; i < N_CORDINATORS; i++) {
      coordinators.add(system.actorOf(TxnCoordinator.props(i), "coordinator" + i));
    }

    // create client and put them to a list

    List<ActorRef> clients = new ArrayList<>();
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(TxnClient.props(i), "client" + i));
    }
    // start cooordinator
    StartMsg startMsg = new StartMsg(servers);
    for (ActorRef coordinator : coordinators)
      coordinator.tell(startMsg, ActorRef.noSender());
    // start client
    WelcomeMsg welcomeMsg = new WelcomeMsg(N_SERVERS * 10 - 1, coordinators);
    for (ActorRef client : clients)
      client.tell(welcomeMsg, ActorRef.noSender());
    int numberOfTest = 0;
    try {
      do {
        System.out.println(">>> Press 1 to make a sum test and ENTER to exit <<<");
        int inChar = System.in.read();
        if (inChar == '1') {
          for (ActorRef server : servers) {
            server.tell(new SumTestRequest(numberOfTest), ActorRef.noSender());
          }
          numberOfTest++;
        } else
          break;
      } while (true);

    } catch (IOException ioe) {
    }
    for (ActorRef server : servers)
      server.tell(new SumTestRequest(numberOfTest), ActorRef.noSender());
    system.terminate();
  }
}
