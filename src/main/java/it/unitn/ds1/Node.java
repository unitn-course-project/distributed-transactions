package it.unitn.ds1;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import scala.concurrent.duration.Duration;

/**
 * Abstract class for basic crash simulation
 */
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

  void crash(int recoverIn) {
    getContext().become(crashed());
    print("CRASH!!!");
    getContext().system().scheduler().scheduleOnce(Duration.create(recoverIn, TimeUnit.MILLISECONDS), getSelf(),
        new Recovery(), getContext().system().dispatcher(), getSelf());
  }

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