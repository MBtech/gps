package gps.messages.storage;

import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

import gps.graph.Graph;
import gps.node.MinaWritableIterable;
import gps.node.Utils;
import gps.node.worker.GPSWorkerExposedGlobalVariables;
import gps.writable.MinaWritable;

/**
 * Stores incoming messages in an array. For each queue, the first value in the queue is the index
 * of the number of messages in the queue, which might be different than the length of the array.
 * 
 * TODO(semih): When implementing greedy dynamism, extend this with two byte arrays to keep which
 * machines the messages are coming from.
 * 
 * @author semihsalihoglu
 */
public class ArrayBackedIncomingMessageStorage<M extends MinaWritable>
	extends IncomingMessageStorage<M> {

	private static Logger logger = Logger.getLogger(ArrayBackedIncomingMessageStorage.class);

	private byte[][] incomingEvenSuperstepDataMessageQueueMap;
	private int[] incomingEvenSuperstepDataMessageIndices;
	private byte[][] incomingOddSuperstepDataMessageQueueMap;
	private int[] incomingOddSuperstepDataMessageIndices;

	private byte[][] previousSuperstepIncomingMessageQueueMap;
	private int[] previousSuperstepIncomingMessageIndices;
	private byte[][] nextSuperstepIncomingMessageQueueMap;
	private int[] nextSuperstepIncomingMessageIndices;
	public static MinaWritableIterable incomingMessageValues = new MinaWritableIterable();
	private final Graph graphPartition;
	private final M representativeMessageInstance;
	private byte[] tmpByteArray = new byte[200];

	private final boolean combine;
	
	@SuppressWarnings("unchecked")
	// WARNING: Make sure that the graphPartition is not used anywhere other than the
	// constructor.
	public ArrayBackedIncomingMessageStorage(Graph graphPartition,
		Class<M> representativeMessageClass, boolean combine, int numWorkers)
		throws InstantiationException, IllegalAccessException {
		this.graphPartition = graphPartition;
		this.combine = combine;
		this.representativeMessageInstance = representativeMessageClass.newInstance();
		this.incomingMessageValues.messagesIterator.setRepresentativeWritableInstance(
			representativeMessageClass.newInstance());
		int dataStructureSizes = graphPartition.size() + 100;
		logger.info("Data Structure Sizes: " + dataStructureSizes);

		incomingEvenSuperstepDataMessageQueueMap = new byte[dataStructureSizes][];
		incomingEvenSuperstepDataMessageIndices = new int[dataStructureSizes];
		incomingOddSuperstepDataMessageQueueMap = new byte[dataStructureSizes][];
		incomingOddSuperstepDataMessageIndices = new int[dataStructureSizes];
		// Note: This class assumes that the localIds, at the time of the construction
		// have been assigned from 0 upto the number of total vertices in the graph partition.
		for (int localId = 0; localId < graphPartition.size(); ++localId) {
			addNewVertexToStorage(localId);
		}
	}
	
	@Override
	public void addMessageToQueue(int toNodeId, IoBuffer ioBuffer) {
		addMessage(toNodeId, ioBuffer);
	}

	public void setNextSuperstepQueueMapAndIndices(int superstepNo) {
		if (previousSuperstepIncomingMessageIndices != null) {
			int messageNumBytes = 2 * representativeMessageInstance.numBytes();
			for (int i = 0; i < previousSuperstepIncomingMessageIndices.length; ++i) {
				previousSuperstepIncomingMessageIndices[i] = 0;
				if (previousSuperstepIncomingMessageQueueMap[i] == null
						|| previousSuperstepIncomingMessageQueueMap[i].length > messageNumBytes) {
					previousSuperstepIncomingMessageQueueMap[i] =
						new byte[messageNumBytes];
				}
			}
		}
		if (Utils.isEven(superstepNo)) {
			nextSuperstepIncomingMessageQueueMap = incomingOddSuperstepDataMessageQueueMap;
			nextSuperstepIncomingMessageIndices = incomingOddSuperstepDataMessageIndices;
		} else {
			nextSuperstepIncomingMessageQueueMap = incomingEvenSuperstepDataMessageQueueMap;
			nextSuperstepIncomingMessageIndices = incomingEvenSuperstepDataMessageIndices;
		}
	}

	private void addMessage(int toNodeId, IoBuffer ioBuffer) {
		int localId = graphPartition.getLocalId(toNodeId);
		if (localId >= nextSuperstepIncomingMessageQueueMap.length) {
			logger.error("localId: " + toNodeId + " is larger than the message queue length: " + nextSuperstepIncomingMessageQueueMap.length);
			representativeMessageInstance.read(ioBuffer);
			return;
		}
		byte[] messageQueue = nextSuperstepIncomingMessageQueueMap[localId];
		if (messageQueue == null) {
			// TODO(semih): Add a message queu here! And change setNextSuperstep.
			logger.error("No message queue for nodeId: " + toNodeId + " localId: " + localId);
			representativeMessageInstance.read(ioBuffer);
			return;
		}
		int messageQueueIndex = nextSuperstepIncomingMessageIndices[localId];
		// TODO(semih): This won't work with using IoBuffers. Fix this.
		if (combine && messageQueueIndex > 0) {
			representativeMessageInstance.combine(messageQueue, tmpByteArray);
			return;
		}
		if (representativeMessageInstance.hasFixedSize()) {
			// TODO(semih): This 64 byte buffer may not be very large
			boolean expandedMessageQueue = false;
			int possibleNewArrayLengthTmp = -1;
			int newArrayLengthTmp = -1;
			if (messageQueueIndex >= (messageQueue.length - 64)) {
				expandedMessageQueue = true;
				int possibleNewArrayLength = messageQueue.length > 80000 ? (int) (messageQueue.length * 1.2)
					: Math.max(messageQueue.length * 2, 64);
				possibleNewArrayLengthTmp = possibleNewArrayLength;
				int newArrayLength = Math.max(2, possibleNewArrayLength);
				newArrayLengthTmp = newArrayLength;
				byte[] newQueue = null;
				try {
					newQueue = new byte[newArrayLength];
				} catch (OutOfMemoryError e) {
					logger.error("ERROR!!! " + e.getMessage());
					e.printStackTrace();
					logger.info("currentMessageQueueLength: " + messageQueue.length +
						" possibleNewArrayLengthTmp: " + possibleNewArrayLengthTmp + " toNodeId: " + toNodeId);
					representativeMessageInstance.read(ioBuffer);
					return;
				}
				System.arraycopy(messageQueue, 0, newQueue, 0, messageQueueIndex);
				nextSuperstepIncomingMessageQueueMap[localId] = newQueue;
				messageQueue = newQueue;
			}
			int messageQueueLength = messageQueue.length;
			int messageQueueIndexTmp = messageQueueIndex;
			try { 
			nextSuperstepIncomingMessageIndices[localId] += representativeMessageInstance.read(
				ioBuffer, messageQueue, messageQueueIndex);
			} catch (IndexOutOfBoundsException e) {
				System.out.println("messageQueue.length: " + messageQueueLength
					+ " messageQueueIndex: " + messageQueueIndexTmp
					+ " expandedMessageQueue: " + expandedMessageQueue
					+ " possibleNewArrayLengthTmp: " + possibleNewArrayLengthTmp
					+ " newArrayLengthTmp: " + newArrayLengthTmp);
				return;	
			}
		} else {
			representativeMessageInstance.read(ioBuffer);
			if (messageQueueIndex >= (messageQueue.length - representativeMessageInstance.numBytes())) {
				int possibleNewArrayLength = messageQueue.length > 80000 ? 
					(int) (messageQueue.length * 1.2) : Math.max(messageQueue.length * 2, 2);
				int newArrayLength = Math.max(messageQueueIndex + representativeMessageInstance.numBytes(),
					possibleNewArrayLength);
				byte[] newQueue = new byte[newArrayLength];
				System.arraycopy(messageQueue, 0, newQueue, 0, messageQueueIndex);
				nextSuperstepIncomingMessageQueueMap[localId] = newQueue;
				messageQueue = newQueue;
			}
			nextSuperstepIncomingMessageIndices[localId] += representativeMessageInstance.read(
				ioBuffer, messageQueue, messageQueueIndex);
		}
	}

	@Override
	public void addMessageToQueues(List<Integer> toNodeIds,
		IoBuffer ioBuffer) {
		int size = representativeMessageInstance.read(ioBuffer, tmpByteArray, 0);
		if (toNodeIds == null || toNodeIds.isEmpty()) {
			return;
		}
		int localId;
		byte[] messageQueue;
		int messageQueueIndex;
		for (int neighborId : toNodeIds) {
			localId = graphPartition.getLocalId(neighborId);
			messageQueue = nextSuperstepIncomingMessageQueueMap[localId];
			if (messageQueue == null) {
				continue;
			}
			messageQueueIndex = nextSuperstepIncomingMessageIndices[localId];
			if (combine && messageQueueIndex > 0) {
				representativeMessageInstance.combine(messageQueue, tmpByteArray);
				return;
			}
			// TODO(semih): This 64 byte buffer may not be very large
			if (messageQueueIndex >= (messageQueue.length - 64)) {
				int possibleNewArrayLength =
					messageQueue.length > 80000 ? (int) (messageQueue.length * 1.2)
						: Math.max(messageQueue.length * 2, 64);
				int newArrayLength = Math.max(2, possibleNewArrayLength);
				byte[] newQueue = new byte[newArrayLength];
				System.arraycopy(messageQueue, 0, newQueue, 0, messageQueueIndex);
				nextSuperstepIncomingMessageQueueMap[localId] = newQueue;
				messageQueue = newQueue;
			}
			System.arraycopy(tmpByteArray, 0, messageQueue, messageQueueIndex, size);
			nextSuperstepIncomingMessageIndices[localId] += size;
		}
	}

	// Warning: We are implicitly assuming that this method will be called once per superstep.
	// That is why we set the index to 0 after constructing the Iterable<Double> return value.
	@Override
	public Iterable<M> getMessageValuesForCurrentSuperstep(int localId) {
		return getMessageValuesFromMessagesQueue(localId, previousSuperstepIncomingMessageQueueMap,
			previousSuperstepIncomingMessageIndices);
	}

	@Override
	public Iterable<M> getMessageValuesForNextSuperstep(int localId) {
		return getMessageValuesFromMessagesQueue(localId, nextSuperstepIncomingMessageQueueMap,
			nextSuperstepIncomingMessageIndices);
	}
	
	public void adjustArraySizesForNewVertex(int idOfVertexReceivedInPreviousSuperstep,
		int superstepNo) {
		int localId = graphPartition.getLocalId(idOfVertexReceivedInPreviousSuperstep);
		int currentSize;
		// We assume that this method is being called at the end of the superstep for a vertex.
		// The reason we are picking the odd superstep as the indication for the length of the
		// message queues when the current superstep is even is the following:
		// - Assume vertex x was moved to this machine in previous superstep (let's say it was odd)
		// - So in this superstep (even), other machines start sending messages to this machine for x
		// - The messages sent in this superstep (even) is for next superstep (odd).
		// Therefore we should look at the length of the message queue for the odd superstep
		// if the current superstep is even and vice versa for the other case.
		if (Utils.isEven(superstepNo)) {
			currentSize = incomingOddSuperstepDataMessageIndices[localId];
			byte[] newOddMessagesQueue = new byte[currentSize];
			byte[] oldOddSuperstepDataMessagesQueue =
				incomingOddSuperstepDataMessageQueueMap[localId];
			System.arraycopy(oldOddSuperstepDataMessagesQueue, 0, newOddMessagesQueue,
				0, Math.min(currentSize, oldOddSuperstepDataMessagesQueue.length));
			incomingEvenSuperstepDataMessageQueueMap[localId] = new byte[currentSize];
		} else {
			currentSize = incomingEvenSuperstepDataMessageIndices[localId];
			byte[] newEvenMessagesQueue = new byte[currentSize];
			byte[] oldEvenSuperstepDataMessagesQueue =
				incomingEvenSuperstepDataMessageQueueMap[localId];
			System.arraycopy(oldEvenSuperstepDataMessagesQueue, 0, newEvenMessagesQueue,
				0, Math.min(currentSize, oldEvenSuperstepDataMessagesQueue.length));
			incomingOddSuperstepDataMessageQueueMap[localId] = new byte[currentSize];
		}
	}

	private Iterable<M> getMessageValuesFromMessagesQueue(int localNodeId,
		byte[][] incomingMessageQueueMap, int[] incomingMessageIndices) {
		incomingMessageValues.messagesIterator.init(incomingMessageQueueMap[localNodeId],
			incomingMessageIndices[localNodeId]);
		incomingMessageIndices[localNodeId] = 0;
		incomingMessageQueueMap[localNodeId] =
			new byte[2*representativeMessageInstance.numBytes()];
		return incomingMessageValues;
	}

	// WARNING: numMessagesReceived is not the exact value. It might be off a little because
	// this class does not check whether an incoming message belongs to the current superstep
	// or the next superstep, which can happen if another GPS Worker starts its superstep
	// computation earlier than this instance and starts sending messages. However it should not
	// be off by much.
	@Override
	public void startingSuperstep() {
		if (Utils.isEven(GPSWorkerExposedGlobalVariables.getCurrentSuperstepNo())) {
			previousSuperstepIncomingMessageQueueMap = incomingEvenSuperstepDataMessageQueueMap;
			previousSuperstepIncomingMessageIndices = incomingEvenSuperstepDataMessageIndices;
		} else {
			previousSuperstepIncomingMessageQueueMap = incomingOddSuperstepDataMessageQueueMap;
			previousSuperstepIncomingMessageIndices = incomingOddSuperstepDataMessageIndices;
		}
	}

	public byte[][] getIncomingMessagesForNextSuperstep() {
		return nextSuperstepIncomingMessageQueueMap;
	}

	public int[] getIncomingIndicesForNextSuperstep() {
		return nextSuperstepIncomingMessageIndices;
	}

	@Override
	public void addVertexToStorage(int localId, int numNeighbors) {
		synchronized (this) {
			if (localId >= incomingEvenSuperstepDataMessageQueueMap.length) {
				int newArrayLength =
					incomingEvenSuperstepDataMessageQueueMap.length > 1000000 ?
						(int) (incomingEvenSuperstepDataMessageQueueMap.length * 1.2)
						: incomingEvenSuperstepDataMessageQueueMap.length * 2;
				incomingEvenSuperstepDataMessageQueueMap =
					resizeDoubleQueueMap(incomingEvenSuperstepDataMessageQueueMap, newArrayLength);
				incomingEvenSuperstepDataMessageIndices =
					resizeIntArray(incomingEvenSuperstepDataMessageIndices, newArrayLength);
				incomingOddSuperstepDataMessageQueueMap =
					resizeDoubleQueueMap(incomingOddSuperstepDataMessageQueueMap, newArrayLength);
				incomingOddSuperstepDataMessageIndices =
					resizeIntArray(incomingOddSuperstepDataMessageIndices, newArrayLength);
			}
			addNewVertexToStorage(localId);
		}
	}

	private int[] resizeIntArray(int[] intArray, int newArrayLength) {
		int[] newArray = new int[newArrayLength];
		for (int i = 0; i < intArray.length; ++i) {
			newArray[i] = intArray[i];
		}
		return newArray;
	}

	private byte[][] resizeDoubleQueueMap(byte[][] queueMap, int newArrayLength) {
		byte[][] newQueueMap = new byte[newArrayLength][];
		for (int i = 0; i < queueMap.length; ++i) {
			newQueueMap[i] = queueMap[i];
		}
		return newQueueMap;
	}

	private void addNewVertexToStorage(int localId) {
		incomingEvenSuperstepDataMessageQueueMap[localId] =
			new byte[2 *representativeMessageInstance.numBytes()];
		incomingEvenSuperstepDataMessageIndices[localId] = 0;
		incomingOddSuperstepDataMessageQueueMap[localId] =
			new byte[2 *representativeMessageInstance.numBytes()];
		incomingOddSuperstepDataMessageIndices[localId] = 0;
	}
}