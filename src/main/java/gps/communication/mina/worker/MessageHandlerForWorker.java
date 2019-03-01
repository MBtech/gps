package gps.communication.mina.worker;

import java.util.concurrent.BlockingQueue;

import gps.communication.mina.BaseMessageHandler;
import gps.communication.mina.BaseMinaMessageSenderAndReceiver;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.OutgoingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.ControlMessagesStats.ControlMessageType;

import org.apache.log4j.Logger;

public class MessageHandlerForWorker extends BaseMessageHandler {

	private static final Logger logger = Logger.getLogger(MessageHandlerForWorker.class);

	protected final BlockingQueue<IncomingBufferedMessage> incomingBufferedDataAndControlMessages;
	private final MinaMessageSenderAndReceiverForWorker minaMessageSenderAndReceiver;

	public MessageHandlerForWorker(
		MinaMessageSenderAndReceiverForWorker minaMessageSenderAndReceiver,
		BlockingQueue<IncomingBufferedMessage> incomingBufferedDataAndControlMessages,
		ControlMessagesStats controlMessageStats,
		BlockingQueue<IncomingBufferedMessage> gpsWorkerMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier) {
		super(controlMessageStats, gpsWorkerMessages, gpsNodeExceptionNotifier);
		this.minaMessageSenderAndReceiver = minaMessageSenderAndReceiver;
		this.incomingBufferedDataAndControlMessages = incomingBufferedDataAndControlMessages;
	}

	@Override
	public void handleMessageReceived(IncomingBufferedMessage incomingBufferedMessage,
		MessageTypes type) throws Exception {
		switch (type) {
		case BEGIN_NEXT_SUPERSTEP_OR_TERMINATE:
			controlMessageStats.addBooleanValueControlMessage(
				incomingBufferedMessage.getSuperstepNo(),
				ControlMessageType.RECEIVED_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_MESSAGE,
				incomingBufferedMessage.getFromMachineId(),
				incomingBufferedMessage.getIoBuffer().get() == 1);
			break;
		case INPUT_SPLIT:
			logger.debug("Received the input split message. Putting it into gpsWorkerMessages.");
			gpsWorkerMessages.put(incomingBufferedMessage);
			logger.debug("Put input split message into gpsWorkerMessages");
			break;
		default:
			logger.debug("Putting a message of type: " + incomingBufferedMessage.getType()
				+ " superstepno: " + incomingBufferedMessage.getSuperstepNo()
				+ " to incomingBufferedMessages" + " from machineid: "
				+ incomingBufferedMessage.getFromMachineId());
			incomingBufferedDataAndControlMessages.put(incomingBufferedMessage);
		}
	}

	@Override
	protected void handleMessageSent(OutgoingBufferedMessage outgoingBufferedMessage,
		MessageTypes type, Integer toMachineId) {
		logger.debug("MESSAGE SENT TO: " + toMachineId + " TYPE: " + type);
		minaMessageSenderAndReceiver.decrementNumOutgoingBuffers();
		switch (type) {
		case FINAL_DATA_SENT:
			controlMessageStats.addPerSuperstepControlMessage(
				outgoingBufferedMessage.getSuperstepNo(),
				ControlMessageType.SENT_FINAL_DATA_SENT_MESSAGES, toMachineId);
			break;
		default:
			// Nothing to do.
		}
	}

	@Override
	public BaseMinaMessageSenderAndReceiver getMessageSenderAndReceiver() {
		return minaMessageSenderAndReceiver;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}
