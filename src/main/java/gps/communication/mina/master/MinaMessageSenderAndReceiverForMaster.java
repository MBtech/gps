package gps.communication.mina.master;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import gps.communication.BaseMessageSenderAndReceiver;
import gps.communication.mina.BaseMinaMessageSenderAndReceiver;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.messages.OutgoingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;
import gps.node.MachineStats;

public class MinaMessageSenderAndReceiverForMaster extends BaseMinaMessageSenderAndReceiver
	implements BaseMessageSenderAndReceiver {

	private static Logger logger = Logger.getLogger(MinaMessageSenderAndReceiverForMaster.class);

	public MinaMessageSenderAndReceiverForMaster(MachineConfig machineConfig, int localMachineId,
		ControlMessagesStats controlMessageStats, MachineStats machineStatsForMaster,
		long connectionFailurePollingTime,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, machineStatsForMaster);
		this.serverConnectionsEstablisher = new ServerConnectionsEstablisherForMaster(machineConfig,
			localMachineId, this, controlMessageStats, machineStatsForMaster,
			globalObjectsMessages, gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
		this.clientConnectionsEstablisher = new ClientConnectionsEstablisherForMaster(machineConfig,
			localMachineId, this, controlMessageStats, machineStatsForMaster,
			connectionFailurePollingTime, globalObjectsMessages,
			gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
	}

	@Override
	public void sendBufferedMessage(OutgoingBufferedMessage outgoingBufferedMessage,
		int toMachineId) {
		sessionsMap.get(toMachineId).write(outgoingBufferedMessage);
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}