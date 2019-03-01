package gps.communication.mina.worker;

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.service.IoHandler;

import gps.communication.mina.ServerConnectionsEstablisher;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;

public class ServerConnectionsEstablisherForWorker extends ServerConnectionsEstablisher {

	protected final BlockingQueue<IncomingBufferedMessage> incomingBufferedDataOrControlMessages;
	private MessageHandlerForWorker messageHandlerForWorker;
	
	public ServerConnectionsEstablisherForWorker(MachineConfig machineConfig, int localMachineId,
		MinaMessageSenderAndReceiverForWorker minaMessageSenderAndReceiverForWorker,
		BlockingQueue<IncomingBufferedMessage> incomingBufferedDataOrControlMessages,
		ControlMessagesStats controlMessageStats,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier,
		MessageHandlerForWorker messageHandlerForWorker, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, minaMessageSenderAndReceiverForWorker,
			controlMessageStats, gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
		this.incomingBufferedDataOrControlMessages = incomingBufferedDataOrControlMessages;
		this.messageHandlerForWorker = messageHandlerForWorker;
	}

	@Override
	protected IoHandler getIoHandler() {
		return this.messageHandlerForWorker;
	}
}