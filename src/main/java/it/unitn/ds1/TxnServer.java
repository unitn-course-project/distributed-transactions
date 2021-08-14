package it.unitn.ds1;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.model.RowValue;

public class TxnServer extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	
	private int id;
	private Map<String, RowValue> data;

	public TxnServer(int id) {
		this.id = id;
	}

	static public Props props(int id) {
		return Props.create(TxnServer.class, () -> new TxnServer(id));
	}


	@Override
	public Receive createReceive() {
		// TODO Auto-generated method stub
		return receiveBuilder().build();
	}

}
