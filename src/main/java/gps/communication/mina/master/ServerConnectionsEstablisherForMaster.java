package gps.communication.mina.master;

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.service.IoHandler;

import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.communication.mina.ServerConnectionsEstablisher;
import gps.messages.IncomingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;
import gps.node.MachineStats;

public class ServerConnectionsEstablisherForMaster extends ServerConnectionsEstablisher {

	private final MachineStats machineStatsForMaster;
	private final BlockingQueue<IncomingBufferedMessage> globalObjectsMessages;

	public ServerConnectionsEstablisherForMaster(MachineConfig machineConfig, int localMachineId,
		MinaMessageSenderAndReceiverForMaster minaMessageSenderAndReceiverForMaster,
		ControlMessagesStats controlMessageStats, MachineStats machineStatsForMaster,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, minaMessageSenderAndReceiverForMaster,
			controlMessageStats, gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
		this.machineStatsForMaster = machineStatsForMaster;
		this.globalObjectsMessages = globalObjectsMessages;
	}

	@Override
	protected IoHandler getIoHandler() {
		return new MessageHandlerForMaster(
			(MinaMessageSenderAndReceiverForMaster) baseMinaMessageSenderAndReceiver,
			controlMessageStats, machineStatsForMaster, globalObjectsMessages,
			gpsNodeExceptionNotifier);
	}
}
