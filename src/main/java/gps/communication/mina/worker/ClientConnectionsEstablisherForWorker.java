package gps.communication.mina.worker;

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.service.IoHandler;

import gps.communication.mina.ClientConnectionsEstablisher;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;

public class ClientConnectionsEstablisherForWorker extends ClientConnectionsEstablisher {

	protected final BlockingQueue<IncomingBufferedMessage> incomingBufferedDataOrControlMessages;
	private MessageHandlerForWorker messageHandlerForWorker;
	
	public ClientConnectionsEstablisherForWorker(MachineConfig machineConfig, int localMachineId,
		MinaMessageSenderAndReceiverForWorker minaMessageSenderAndReceiverForWorker,
		BlockingQueue<IncomingBufferedMessage> incomingBufferedDataOrControlMessages,
		ControlMessagesStats controlMessageStats, long connectionFailurePollingTime,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier,
		MessageHandlerForWorker messageHandlerForWorker, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, minaMessageSenderAndReceiverForWorker, 
			controlMessageStats, connectionFailurePollingTime, gpsNodeExceptionNotifier,
			numProcessorsForHandlingIO);
		this.incomingBufferedDataOrControlMessages = incomingBufferedDataOrControlMessages;
		this.messageHandlerForWorker = messageHandlerForWorker;
	}

	@Override
	public IoHandler getIoHandler() {
		return messageHandlerForWorker;
	}
}
