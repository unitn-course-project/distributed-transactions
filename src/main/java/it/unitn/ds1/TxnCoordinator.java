package it.unitn.ds1;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
/**
 *  Coordinators manage transaction request and manipulate it among servers
 */
public class TxnCoordinator extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private int id;
	private List<ActorRef> servers;
	
	
	public TxnCoordinator(int id) {
		this.id = id;
	}


	static public Props props(int id) {
		return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(id));
	}
	@Override
	public Receive createReceive() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
