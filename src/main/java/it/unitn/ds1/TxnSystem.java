package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class TxnSystem {
  final static int N_SERVERS = 10;
  final static int N_CORDINATORS = 10;
  final static int N_CLIENTS = 10;

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("tnxsystem");

    // Create servers and put them to a list
    List<ActorRef> servers = new ArrayList<>();
    for (int i=0; i<N_SERVERS; i++) {
      servers.add(system.actorOf(TxnServer.props(i), "server" + i));
    }

    // Create coordinators and put them to a list
    List<ActorRef> coordinator = new ArrayList<>();
    for (int i=0; i<N_CORDINATORS; i++) {
      servers.add(system.actorOf(TxnCoordinator.props(i), "coordinator" + i));
    }

    // create client and put them to a list

    List<ActorRef> clients= new ArrayList<>();
    for (int i=0; i<N_CLIENTS; i++) {
      servers.add(system.actorOf(TxnClient.props(i), "client" + i));
    }

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}
