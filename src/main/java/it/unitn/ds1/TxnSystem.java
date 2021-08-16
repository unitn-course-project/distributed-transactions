package it.unitn.ds1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.TxnClient.WelcomeMsg;
import it.unitn.ds1.TxnCoordinator.StartMsg;

public class TxnSystem {
  final static int N_SERVERS = 10;
  final static int N_CORDINATORS = 10;
  final static int N_CLIENTS = 10;

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

    StartMsg startMsg = new StartMsg(servers);
    for (ActorRef coordinator : coordinators)
      coordinator.tell(startMsg, ActorRef.noSender());

    WelcomeMsg welcomeMsg = new WelcomeMsg(N_SERVERS * 10 - 1, coordinators);
    for (ActorRef client : clients)
      client.tell(welcomeMsg, ActorRef.noSender());

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } catch (IOException ioe) {
    }
    system.terminate();
  }
}
