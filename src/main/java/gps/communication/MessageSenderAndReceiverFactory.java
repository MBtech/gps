package gps.communication;

import java.util.concurrent.BlockingQueue;

import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;
import gps.node.MachineStats;

public interface MessageSenderAndReceiverFactory {

	public MessageSenderAndReceiverForWorker newInstanceForWorker(MachineConfig machineConfig,
		int localMachineId, BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages,
		ControlMessagesStats controlMessageStats, long connectionFailurePollingTime,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessage,
		MachineStats machineStats, GPSNodeExceptionNotifier gpsNodeExceptionNotifier,
		int maxMessagesToTransmitConcurrently, int numProcessorsForHandlingIO);

	public BaseMessageSenderAndReceiver newInstanceForMaster(MachineConfig machineConfig,
		int localMachineId, ControlMessagesStats controlMessageStats,
		MachineStats machineStatsForMaster, long connectionFailurePollingTime,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO);
}
