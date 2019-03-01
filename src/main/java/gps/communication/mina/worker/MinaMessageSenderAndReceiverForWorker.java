package gps.communication.mina.worker;

import gps.communication.MessageSenderAndReceiverForWorker;
import gps.communication.mina.BaseMinaMessageSenderAndReceiver;
import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.MessageUtils;
import gps.messages.OutgoingBufferedMessage;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.StatusType;
import gps.node.Utils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

/**
 * Implementation of {@link MessageSenderAndReceiverForWorker} that uses MINA as the underlying
 * network layer.
 * 
 * @author semihsalihoglu
 */
public class MinaMessageSenderAndReceiverForWorker extends BaseMinaMessageSenderAndReceiver
	implements MessageSenderAndReceiverForWorker {

	private static Logger logger = Logger.getLogger(MinaMessageSenderAndReceiverForWorker.class);

	private final BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages;
	private final LinkedList<OutgoingBufferedMessageWrapper> outgoingBufferedMessageWrappers;
	private int numMessagesBeingTransmitted;
	private int maxMessagesToTransmitConcurrently;
	private Integer numNetworkBuffersSent = 0;
	private Integer sentNetworkBufferToMachineId = null;
	static private Long totalNumNetworkBuffersSentTime = (long) 0;
	static private Long previousNetworkBufferSendingTime = null;
	static private long timeTakenToSendDataMessage;
	static public long[] numNetworkBuffersSentToEachWorker;
	static public long[] totalDataMessageSendingTimesForEachWorker;
	
	public MinaMessageSenderAndReceiverForWorker(MachineConfig machineConfig, int localMachineId,
		BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages,
		ControlMessagesStats controlMessageStats, long connectionFailurePollingTime,
		BlockingQueue<IncomingBufferedMessage> globalObjectsMessages,
		MachineStats machineStats, GPSNodeExceptionNotifier gpsNodeExceptionNotifier,
		int maxMessagesToTransmitConcurrently, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, machineStats);
		this.incomingBufferedMessages = incomingBufferedMessages;
		this.outgoingBufferedMessageWrappers = new LinkedList<OutgoingBufferedMessageWrapper>();
		this.numMessagesBeingTransmitted = 0;
		this.maxMessagesToTransmitConcurrently = maxMessagesToTransmitConcurrently;
		MessageHandlerForWorker messageHandlerForWorker =  new MessageHandlerForWorker(
			this, incomingBufferedMessages, controlMessageStats,
			globalObjectsMessages, gpsNodeExceptionNotifier);
		this.serverConnectionsEstablisher = new ServerConnectionsEstablisherForWorker(machineConfig,
			localMachineId, this, incomingBufferedMessages, controlMessageStats,
			gpsNodeExceptionNotifier, messageHandlerForWorker, numProcessorsForHandlingIO);
		this.clientConnectionsEstablisher = new ClientConnectionsEstablisherForWorker(machineConfig,
			localMachineId, this, incomingBufferedMessages, controlMessageStats,
			connectionFailurePollingTime, gpsNodeExceptionNotifier, messageHandlerForWorker,
			numProcessorsForHandlingIO);
		this.totalDataMessageSendingTimesForEachWorker = new long[machineConfig.getWorkerIds().size()];
		this.numNetworkBuffersSentToEachWorker = new long[machineConfig.getWorkerIds().size()];
	}

	@Override
	public void sendFinalDataSentControlMessagesToAllWorkers(int superstepNo) {
		int[] randomPermutation = Utils.getRandomPermutation(machineConfig.getWorkerIds()
			.size());
		List<Integer> allWorkerIds = new LinkedList<Integer>(machineConfig.getWorkerIds());
		for (int i : randomPermutation) {
			int toMachineId = allWorkerIds.get(i);
			logger.debug("Sending " + MessageTypes.FINAL_DATA_SENT + " message toMachineId:"
				+ toMachineId);
			sendBufferedMessage(MessageUtils.constructMessage(
				MessageTypes.FINAL_DATA_SENT, superstepNo), toMachineId);
		}
	}

	@Override
	public void sendBufferedMessage(OutgoingBufferedMessage outgoingBufferedMessage,
		int toMachineId) {
		if (toMachineId == localMachineId) {
			if (outgoingBufferedMessage.getIoBuffer() != null) {
				outgoingBufferedMessage.getIoBuffer().flip();
			}
			IncomingBufferedMessage incomingBufferedMessage = new IncomingBufferedMessage(
				outgoingBufferedMessage.getType(), outgoingBufferedMessage.getSuperstepNo(),
				outgoingBufferedMessage.getIoBuffer());
			incomingBufferedMessage.setFromMachineId(localMachineId);
			logger.debug("Putting a local message of type: " + incomingBufferedMessage.getType()
				+ " superstepNo: " + incomingBufferedMessage.getSuperstepNo()
				+ " to incomingBufferedMessages" + " from machineId: " + localMachineId);
			incomingBufferedMessages.add(incomingBufferedMessage);
		} else {
			synchronized (MinaMessageSenderAndReceiverForWorker.class) {
				if (numMessagesBeingTransmitted < maxMessagesToTransmitConcurrently) {
					numMessagesBeingTransmitted++;
					checkIfOutgoingMessageShouldBeTimed(outgoingBufferedMessage, toMachineId);
					sessionsMap.get(toMachineId).write(outgoingBufferedMessage);
				} else {
					outgoingBufferedMessageWrappers.add(new OutgoingBufferedMessageWrapper(
						outgoingBufferedMessage, toMachineId));
				}
			}
		}
	}

	private void checkIfOutgoingMessageShouldBeTimed(
		OutgoingBufferedMessage outgoingBufferedMessage, int toMachineId) {
		if ((MessageTypes.DATA == outgoingBufferedMessage.getType()) ||
			(MessageTypes.FINAL_DATA_SENT == outgoingBufferedMessage.getType()) ||
			(MessageTypes.FINISHED_PARSING_DATA_MESSAGES == outgoingBufferedMessage.getType())) {
			if ((MessageTypes.FINAL_DATA_SENT == outgoingBufferedMessage.getType())) {
				logger.debug("Timing FINAL_DATA_SENT message.");
			} else if ((MessageTypes.FINISHED_PARSING_DATA_MESSAGES == outgoingBufferedMessage.getType())) {
				logger.debug("Timing FINISHED_PARSING_DATA_MESSAGES message.");
			}
			previousNetworkBufferSendingTime = System.currentTimeMillis();
			sentNetworkBufferToMachineId = toMachineId;
			
		} else {
			previousNetworkBufferSendingTime = null;
			sentNetworkBufferToMachineId = null;
		}
	}

	public void decrementNumOutgoingBuffers() {
		synchronized (MinaMessageSenderAndReceiverForWorker.class) {
			if (previousNetworkBufferSendingTime != null) {
				timeTakenToSendDataMessage = (System.currentTimeMillis() - previousNetworkBufferSendingTime);
				totalDataMessageSendingTimesForEachWorker[sentNetworkBufferToMachineId] +=
					timeTakenToSendDataMessage;
				numNetworkBuffersSentToEachWorker[sentNetworkBufferToMachineId]++;
				totalNumNetworkBuffersSentTime += timeTakenToSendDataMessage;
			}
			numMessagesBeingTransmitted--;
			numNetworkBuffersSent++;
			if (!outgoingBufferedMessageWrappers.isEmpty()) {
				OutgoingBufferedMessageWrapper outgoingBufferedMessageWrapper =
					outgoingBufferedMessageWrappers.remove(0);
				sessionsMap.get(outgoingBufferedMessageWrapper.toMachineId).write(
					outgoingBufferedMessageWrapper.outgoingBufferedMessage);
				numMessagesBeingTransmitted++;
				checkIfOutgoingMessageShouldBeTimed(outgoingBufferedMessageWrapper.outgoingBufferedMessage,
					outgoingBufferedMessageWrapper.toMachineId);
			}
		}
	}

	public static void dumpAverageSendTimeStatistics() {
		for (int i = 0; i < totalDataMessageSendingTimesForEachWorker.length; ++i) {			
			if (numNetworkBuffersSentToEachWorker[i] == 0) {
				continue;
			}
			logger.debug("worker: " + i + 
				" totalTimeSpentSendingBuffers: " + totalDataMessageSendingTimesForEachWorker[i] +
				" numBuffersSent: " + numNetworkBuffersSentToEachWorker[i] + 
				" averageBufferSendingTime: "
				+ (totalDataMessageSendingTimesForEachWorker[i] / numNetworkBuffersSentToEachWorker[i]));
		}
	}
	
	@Override
	public int getNumOutgoingBuffersInQueue() {
		synchronized (MinaMessageSenderAndReceiverForWorker.class) {
			return outgoingBufferedMessageWrappers.size();			
		}
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public void sendStatusUpdateToMaster(int superstepNo, StatusType statusType) {
		sendBufferedMessage(MessageUtils.constructStatusUpdateMessage(superstepNo, statusType),
			Utils.MASTER_ID);
	}
	
	private static class OutgoingBufferedMessageWrapper {
		private OutgoingBufferedMessage outgoingBufferedMessage;
		private int toMachineId;
		
		private OutgoingBufferedMessageWrapper(OutgoingBufferedMessage outgoingBufferedMessage,
			int toMachineId) {
			this.outgoingBufferedMessage = outgoingBufferedMessage;
			this.toMachineId = toMachineId;
		}
	}

	@Override
	public void sendFinalInitialVertexPartitioningControlMessagesToAllWorkers() {
		int[] randomPermutation = Utils.getRandomPermutation(machineConfig.getWorkerIds()
			.size());
		List<Integer> allWorkerIds = new LinkedList<Integer>(machineConfig.getWorkerIds());
		for (int i : randomPermutation) {
			int toMachineId = allWorkerIds.get(i);
			logger.debug("Sending " + MessageTypes.FINAL_INITIAL_VERTEX_PARTITIONING_SENT +
				" message toMachineId:" + toMachineId);
			sendBufferedMessage(MessageUtils.constructMessage(
				MessageTypes.FINAL_INITIAL_VERTEX_PARTITIONING_SENT, -1), toMachineId);
		}
	}
}
