package it.unitn.ds1;

import java.util.HashMap;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.TxnCoordinator.ReadDataMsg;
import it.unitn.ds1.TxnCoordinator.ReadDataResultMsg;
import it.unitn.ds1.model.RowValue;

public class TxnServer extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	private int id;
	private Map<Integer, RowValue> data;

	public TxnServer(int id) {
		this.id = id;
	}

	static public Props props(int id) {
		return Props.create(TxnServer.class, () -> new TxnServer(id));
	}

	@Override
	public void preStart() {
		data= new HashMap<>();
		for (int i = 10 * id; i < 10 * (id + 1); i++) {
			data.put(i, new RowValue(0, 100));
		}
	}
	/*-- Message classes ------------------------------------------------------ */

	/*-- Message handlers ----------------------------------------------------- */
	private void onReadMsg(ReadDataMsg readMsg) {
		RowValue readValue = data.get(readMsg.key);
		getSender().tell(new ReadDataResultMsg(readMsg.transactionId, readMsg.key, readValue.getValue(),
				readValue.getVersion()), getSelf());
	}

	@Override
	public Receive createReceive() {
		// TODO Auto-generated method stub
		return receiveBuilder().match(ReadDataMsg.class, this::onReadMsg).build();
	}

}
