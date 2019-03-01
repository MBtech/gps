package gps.communication.mina.master;

import java.util.concurrent.BlockingQueue;

import gps.communication.mina.BaseMessageHandler;
import gps.communication.mina.BaseMinaMessageSenderAndReceiver;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.OutgoingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineStats;
import gps.node.MachineStats.StringStatName;
import gps.node.Utils;
import gps.node.ControlMessagesStats.ControlMessageType;
import gps.node.MachineStats.StatName;
import gps.node.StatusType;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

public class MessageHandlerForMaster extends BaseMessageHandler {

	private static final Logger logger = Logger.getLogger(MessageHandlerForMaster.class);

	private final MinaMessageSenderAndReceiverForMaster minaMessageSenderAndReceiver;
	private final MachineStats machineStatsForMaster;

	public MessageHandlerForMaster(
		MinaMessageSenderAndReceiverForMaster minaMessageSenderAndReceiver,
		ControlMessagesStats controlMessageStats, MachineStats machineStatsForMaster,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier) {
		super(controlMessageStats, globalObjectsMessages, gpsNodeExceptionNotifier);
		this.minaMessageSenderAndReceiver = minaMessageSenderAndReceiver;
		this.machineStatsForMaster = machineStatsForMaster;
	}

	@Override
	public void handleMessageReceived(IncomingBufferedMessage incomingBufferedMessage,
		MessageTypes type) throws Exception {
		logger.debug("handleMessageReceived for a messageType: " + type + " fromMachineId: "
			+ incomingBufferedMessage.getFromMachineId() + " superstepNo: "
			+ incomingBufferedMessage.getSuperstepNo());
		switch (type) {
		case STATUS_UPDATE:
			int superstepNo = incomingBufferedMessage.getSuperstepNo();
			int fromMachineId = incomingBufferedMessage.getFromMachineId();
			IoBuffer ioBuffer = incomingBufferedMessage.getIoBuffer();
			StatusType statusType = StatusType.getTypeFromId(ioBuffer.getInt());
			if (StatusType.END_OF_SUPERSTEP == statusType) {
				Boolean isActive = null;
				while (ioBuffer.hasRemaining()) {
					StatName statName = StatName.getStatNameFromId(ioBuffer.getInt());
					double statValue = ioBuffer.getDouble();
					if (StatName.NUM_ACTIVE_NODES_FOR_NEXT_SUPERSTEP == statName) {
						isActive = statValue > 0.0;
					}
					machineStatsForMaster.updateStat(statName, superstepNo, statValue,
						fromMachineId);
				}
				controlMessageStats.addBooleanValueControlMessage(
					incomingBufferedMessage.getSuperstepNo(),
					ControlMessageType.RECEIVED_END_OF_SUPERSTEP_MESSAGES,
					incomingBufferedMessage.getFromMachineId(), (isActive));
			} else if (StatusType.READY_TO_DO_COMPUTATION == statusType) {
				logger.debug("Recieved READY_TO_DO_COMPUTATION message from machineId: "
					+ fromMachineId);
				controlMessageStats.addGlobalControlMessage(
					ControlMessageType.READY_TO_START_COMPUTATION, fromMachineId);
			} else if (StatusType.EXCEPTION_THROWN == statusType) {
				String exceptionStr = ioBuffer.getString(Utils.ISO_8859_1_DECODER);
				logger.error("Exception string received: " + exceptionStr);
				machineStatsForMaster.updateGlobalStat(StringStatName.EXCEPTION_STACK_TRACE,
					fromMachineId, exceptionStr);
			}
			machineStatsForMaster.updateLatestStatus(fromMachineId, statusType);
			break;
		default:
			logger.error("Master: Unexpected received message of type: "
				+ incomingBufferedMessage.getType() + " superstepno: "
				+ incomingBufferedMessage.getSuperstepNo() + " from machineid: "
				+ incomingBufferedMessage.getFromMachineId());
			throw new UnsupportedOperationException("Master should not have received messages of"
				+ " type: " + incomingBufferedMessage.getType());
		}
	}

	@Override
	protected void handleMessageSent(OutgoingBufferedMessage outgoingBufferedMessage,
		MessageTypes type, Integer toMachineId) {
		switch (type) {
		case BEGIN_NEXT_SUPERSTEP_OR_TERMINATE:
			controlMessageStats.addPerSuperstepControlMessage(
				outgoingBufferedMessage.getSuperstepNo(),
				ControlMessageType.SENT_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE, toMachineId);
			break;
		case GLOBAL_OBJECTS:
			// Nothing to do.
			break;
		case INPUT_SPLIT:
			// Nothing to do.
			break;
		default:
			logger.error("Master: Unexpected sent message of type: " + type + " superstepno: "
				+ outgoingBufferedMessage.getSuperstepNo() + " from machineid: " + toMachineId);
			throw new UnsupportedOperationException(
				"Master should not have sent messages of type: " + type);
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
