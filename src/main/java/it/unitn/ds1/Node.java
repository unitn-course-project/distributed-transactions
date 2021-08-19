package it.unitn.ds1;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import scala.concurrent.duration.Duration;

public abstract class Node extends AbstractActor {
  protected int id; // node ID

  public Node(int id) {
    super();
    this.id = id;
  }


  public static class Recovery implements Serializable {
  }

  // abstract method to be implemented in extending classes
  protected abstract void onRecovery(Recovery msg);

  // emulate a crash and a recovery in a given time
  void crash(int recoverIn) {
    getContext().become(crashed());
    print("CRASH!!!");

    // setting a timer to "recover"
    getContext().system().scheduler().scheduleOnce(Duration.create(recoverIn, TimeUnit.MILLISECONDS), getSelf(),
        new Recovery(), // message sent to myself
        getContext().system().dispatcher(), getSelf());
  }

  // emulate a delay of d milliseconds
  void delay(int d) {
    try {
      Thread.sleep(d);
    } catch (Exception ignored) {
    }
  }

  void print(String s) {
    System.out.format("%2d: %s\n", id, s);
  }

  public Receive crashed() {
    return receiveBuilder().match(Recovery.class, this::onRecovery).matchAny(msg -> {
    }).build();
  }

}