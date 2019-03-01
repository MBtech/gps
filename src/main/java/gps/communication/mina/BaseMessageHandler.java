package gps.communication.mina;

import java.util.concurrent.BlockingQueue;

import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.OutgoingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.ControlMessagesStats.ControlMessageType;

import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

public abstract class BaseMessageHandler implements IoHandler {

	protected final ControlMessagesStats controlMessageStats;
	private final GPSNodeExceptionNotifier gpsNodeExceptionNotifier;
	protected final BlockingQueue<IncomingBufferedMessage> gpsWorkerMessages;

	public BaseMessageHandler(ControlMessagesStats controlMessageStats,
		BlockingQueue<IncomingBufferedMessage> gpsWorkerMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier) {
		this.controlMessageStats = controlMessageStats;
		this.gpsNodeExceptionNotifier = gpsNodeExceptionNotifier;
		this.gpsWorkerMessages = gpsWorkerMessages;
	}

//	@Override
	public void messageReceived(IoSession session, Object objectMessage) throws Exception {
		Integer fromMachineId = (Integer) session.getAttribute("machineId");
		IncomingBufferedMessage incomingBufferedMessage = (IncomingBufferedMessage) objectMessage;
		getLogger().debug("Message received from: " + fromMachineId + " type: "
			+ incomingBufferedMessage.getType());
		MessageTypes type = incomingBufferedMessage.getType(); 
		switch (type) {
		case MACHINE_ID_INFORMATION:
			int machineId = incomingBufferedMessage.getIoBuffer().getInt();
			session.setAttribute("machineId", machineId);
			getMessageSenderAndReceiver().putSession(machineId, session);
			break;
		case GLOBAL_OBJECTS:
			incomingBufferedMessage.setFromMachineId(fromMachineId);
			gpsWorkerMessages.put(incomingBufferedMessage);
			controlMessageStats.addPerSuperstepControlMessage(
				incomingBufferedMessage.getSuperstepNo(),
				ControlMessageType.RECEIVED_GLOBAL_OBJECTS_MESSAGES,
				incomingBufferedMessage.getFromMachineId());
			break;
		default:
			incomingBufferedMessage.setFromMachineId(fromMachineId);
			handleMessageReceived(incomingBufferedMessage, type);
		}
	}

//	@Override
	public void messageSent(IoSession session, Object arg1) throws Exception {
		Integer toMachineId = (Integer) session.getAttribute("machineId");
		OutgoingBufferedMessage outgoingBufferedMessage = (OutgoingBufferedMessage) arg1;
		MessageTypes type = outgoingBufferedMessage.getType();
		getLogger().debug("MESSAGE SENT TO: " + toMachineId + " TYPE: " + type);
		switch (type) {
		case MACHINE_ID_INFORMATION:
			// Do nothing.
			break;
		default:
			handleMessageSent(outgoingBufferedMessage, type, toMachineId);
		}
	}

//	@Override
	public void exceptionCaught(IoSession session, Throwable e) throws Exception {
		e.printStackTrace();
		getLogger().error("Exception Caught for " + getClass().getName() + ": " + e.getMessage()
			+ " machineId: " + session.getAttribute("machineId"));
		gpsNodeExceptionNotifier.setThrowable(e);
	}

//	@Override
	public void sessionClosed(IoSession session) throws Exception {
		getLogger().info("session CLOSED: " + session.getCreationTime() + " machineId: "
			+ session.getAttribute("machineId"));
	}

//	@Override
	public void sessionCreated(IoSession session) throws Exception {
		getLogger().info("session CREATED: " + session.getCreationTime() + " machineId: "
			+ session.getAttribute("machineId"));
	}

//	@Override
	public void sessionIdle(IoSession session, IdleStatus arg1) throws Exception {
		getLogger().info("session IDLE: " + session.getBothIdleCount() + " machineId: "
			+ session.getAttribute("machineId"));
	}

//	@Override
	public void sessionOpened(IoSession session) throws Exception {
		getLogger().info("session OPENED: " + " machineId: " + session.getAttribute("machineId"));
	}

	protected abstract void handleMessageReceived(IncomingBufferedMessage incomingBufferedMessage,
		MessageTypes type) throws Exception;

	protected abstract void handleMessageSent(OutgoingBufferedMessage outgoingBufferedMessage,
		MessageTypes type, Integer toMachineId);

	public abstract BaseMinaMessageSenderAndReceiver getMessageSenderAndReceiver();
	
	public abstract Logger getLogger();
}
