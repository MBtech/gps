package gps.communication.mina.master;

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.service.IoHandler;

import gps.communication.mina.ClientConnectionsEstablisher;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;
import gps.node.MachineStats;

public class ClientConnectionsEstablisherForMaster extends ClientConnectionsEstablisher {

	private final MachineStats machineStatsForMaster;
	private final BlockingQueue<IncomingBufferedMessage> globalObjectsMessages;

	public ClientConnectionsEstablisherForMaster(MachineConfig machineConfig, int localMachineId,
		MinaMessageSenderAndReceiverForMaster minaMessageSenderAndReceiverForMaster,
		ControlMessagesStats controlMessageStats, MachineStats machineStatsForMaster,
		long connectionFailurePollingTime, 
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, minaMessageSenderAndReceiverForMaster, 
			controlMessageStats, connectionFailurePollingTime, gpsNodeExceptionNotifier,
			numProcessorsForHandlingIO);
		this.machineStatsForMaster = machineStatsForMaster;
		this.globalObjectsMessages = globalObjectsMessages;
	}

	@Override
	public IoHandler getIoHandler() {
		return new MessageHandlerForMaster(
			(MinaMessageSenderAndReceiverForMaster) baseMinaMessageSenderAndReceiver,
			controlMessageStats, machineStatsForMaster, globalObjectsMessages,
			gpsNodeExceptionNotifier);
	}
}
