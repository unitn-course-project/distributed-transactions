package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Coordinators manage transaction request and manipulate it among servers
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

	/*-- Message classes ------------------------------------------------------ */
	public static class StartMsg implements Serializable {
		public final List<ActorRef> servers;

		public StartMsg(List<ActorRef> servers) {
			this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
		}
	}

	/*-- Message handlers ----------------------------------------------------- */
	private void onStartMsg(StartMsg welcomeMsg) {
		this.servers = welcomeMsg.servers;
		log.info("Coordinator "+id+" recognize "+servers.size()+" servers");
	}

	@Override
	public Receive createReceive() {
		// TODO Auto-generated method stub
		return receiveBuilder().match(StartMsg.class, this::onStartMsg).build();
	}

}
