package gps.communication.mina;

import java.util.concurrent.BlockingQueue;

import gps.communication.BaseMessageSenderAndReceiver;
import gps.communication.MessageSenderAndReceiverForWorker;
import gps.communication.MessageSenderAndReceiverFactory;
import gps.communication.mina.master.MinaMessageSenderAndReceiverForMaster;
import gps.communication.mina.worker.MinaMessageSenderAndReceiverForWorker;
import gps.messages.IncomingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;
import gps.node.MachineStats;

public class MinaMessageSenderAndReceiverFactory implements MessageSenderAndReceiverFactory {

	@Override
	public MessageSenderAndReceiverForWorker newInstanceForWorker(MachineConfig machineConfig,
		int localMachineId, BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages,
		ControlMessagesStats controlMessageStats, long connectionFailurePollingTime,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		MachineStats machineStats, GPSNodeExceptionNotifier gpsNodeExceptionNotifier,
		int maxMessagesToTransmitConcurrently, int numProcessorsForHandlingIO) {
		return new MinaMessageSenderAndReceiverForWorker(machineConfig, localMachineId,
			incomingBufferedMessages, controlMessageStats, connectionFailurePollingTime,
			globalObjectsMessages, machineStats, gpsNodeExceptionNotifier,
			maxMessagesToTransmitConcurrently, numProcessorsForHandlingIO);
	}

	@Override
	public BaseMessageSenderAndReceiver newInstanceForMaster(MachineConfig machineConfig,
		int localMachineId, ControlMessagesStats controlMessageStats,
		MachineStats machineStatsForMaster, long connectionFailurePollingTime,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		return new MinaMessageSenderAndReceiverForMaster(machineConfig, localMachineId,
			controlMessageStats, machineStatsForMaster, connectionFailurePollingTime,
			globalObjectsMessages, gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
	}
}
