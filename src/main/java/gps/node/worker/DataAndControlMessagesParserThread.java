package gps.node.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import gps.globalobjects.GlobalObjectsMap;
import gps.graph.Graph;
import gps.graph.Vertex;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.storage.IncomingMessageStorage;
import gps.node.ControlMessagesStats;
import gps.node.GPSJobConfiguration;
import gps.node.ControlMessagesStats.ControlMessageType;
import gps.node.MachineStats.StatName;
import gps.writable.MinaWritable;
import gps.writable.NullWritable;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

import static gps.node.worker.GPSWorkerExposedGlobalVariables.*;

/**
 * Worker thread that parses both data messages and control messages.
 * 
 * Note: This thread parses local messages as well. This is so to avoid multiple threads accessing
 * message queues, which would require threads acquiring locks.
 * 
 * @author semihsalihoglu
 */
public class  DataAndControlMessagesParserThread<MW extends MinaWritable>
	extends Thread {

	private static Logger logger = Logger.getLogger(DataAndControlMessagesParserThread.class);

	private int latestSuperstepNo = -1;
	private IncomingMessageStorage<MW> incomingMessageStorage;
	private final BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages;
	private final ControlMessagesStats controlMessagesStats;
	private final Graph graphPartition;
	private final Vertex representativeVertex;
	// TODO(semih): Make this private
	public static Map<Integer, List<Integer>> outsidePartitionedVertices =
		new HashMap<Integer, List<Integer>>();
	protected Class<MW> messageRepresentativeClass;
	protected static long[] dataParserTimeSpentOnAddMessages;
	protected static long[] dataParserTimeSpentWaitingForBuffersToArrive;

	private final boolean isNoParsing;

	private final MinaWritable representativeEdgeValue;
	private final Class<? extends MinaWritable> representativeVertexValueClassForInputParsing;

	public DataAndControlMessagesParserThread(IncomingMessageStorage<MW> incomingMessageStorage,
		BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages,
		ControlMessagesStats controlMessagesStats, Class<MW> messageRepresentativeClass,
		Graph graphPartition, Vertex representativeVertex, MinaWritable representativeEdgeValue,
		GPSJobConfiguration jobConfiguration, boolean isNoParsing) throws InstantiationException, IllegalAccessException {
		super("DataAndControlMessagesParserThread");
		this.incomingMessageStorage = incomingMessageStorage;
		this.incomingBufferedMessages = incomingBufferedMessages;
		this.controlMessagesStats = controlMessagesStats;
		this.messageRepresentativeClass = messageRepresentativeClass;
		this.graphPartition = graphPartition;
		this.representativeVertex = representativeVertex;
		this.representativeEdgeValue = representativeEdgeValue;
		if (jobConfiguration.hasVertexValuesInInput()) {
			this.representativeVertexValueClassForInputParsing =
				(Class<? extends MinaWritable>) jobConfiguration.getVertexValueClass();
		} else {
			this.representativeVertexValueClassForInputParsing = null;
		}
		this.isNoParsing = isNoParsing;
		dataParserTimeSpentOnAddMessages = new long[100000];
		dataParserTimeSpentWaitingForBuffersToArrive = new long[100000];
	}

	@Override
	public void run() {
		while (true) {
			try {
				long timeBefore;
				int fromMachineId;
				long timeBeforeTakingMessage = System.currentTimeMillis();
				IncomingBufferedMessage incomingBufferedDataMessage =
					incomingBufferedMessages.take();
				long timeAfterTakingMessage = System.currentTimeMillis();
				MessageTypes type = incomingBufferedDataMessage.getType();
				int superstepNo = incomingBufferedDataMessage.getSuperstepNo();
				if ((superstepNo > 0) && (type == MessageTypes.DATA)) {
					if (dataParserTimeSpentWaitingForBuffersToArrive[superstepNo] == 0) {
						dataParserTimeSpentWaitingForBuffersToArrive[superstepNo] = 1;
					} else {
						dataParserTimeSpentWaitingForBuffersToArrive[superstepNo] +=
						(timeAfterTakingMessage - timeBeforeTakingMessage);
					}
				}
				if (superstepNo > latestSuperstepNo) {
					latestSuperstepNo = superstepNo;
					getMachineStats().updateStatForSuperstep(
						StatName.DATA_PARSER_FIRST_MESSAGE_PARSING_TIME, superstepNo,
						(double) System.currentTimeMillis());
				}
				IoBuffer ioBuffer = null;
				switch (type) {
				case DATA:
					if (!isNoParsing) {
						timeBefore = System.currentTimeMillis();
						ioBuffer = incomingBufferedDataMessage.getIoBuffer();
						fromMachineId = incomingBufferedDataMessage.getFromMachineId();
						int toNodeId;
						while (ioBuffer.hasRemaining()) {
							toNodeId = ioBuffer.getInt();
							if (toNodeId >= 0) {
								incomingMessageStorage.addMessageToQueue(toNodeId, ioBuffer);
							} else {
								incomingMessageStorage.addMessageToQueues(
									outsidePartitionedVertices.get(toNodeId * -1), ioBuffer);
							}
						}
						dataParserTimeSpentOnAddMessages[superstepNo] +=
							(System.currentTimeMillis() - timeBefore);
					}
					break;
				case LARGE_VERTEX_DATA:
					logger.info("Parsing LARGE_VERTEX_DATA message...");
					timeBefore = System.currentTimeMillis();
					ioBuffer = incomingBufferedDataMessage.getIoBuffer();
					fromMachineId = incomingBufferedDataMessage.getFromMachineId();
					int fromNodeId;
					while (ioBuffer.hasRemaining()) {
						fromNodeId = ioBuffer.getInt();
						incomingMessageStorage.addMessageToQueues(
							outsidePartitionedVertices.get(fromNodeId), ioBuffer);
					}
					getMachineStats().updateStatForSuperstep(
						StatName.DATA_PARSER_TIME_SPENT_ON_ADD_MESSAGE_TO_QUEUES, superstepNo,
						(double) (System.currentTimeMillis() - timeBefore));
					break;
				case LARGE_VERTEX_PARTITIONS:
					timeBefore = System.currentTimeMillis();
					ioBuffer = incomingBufferedDataMessage.getIoBuffer();
					fromMachineId = incomingBufferedDataMessage.getFromMachineId();
					while (ioBuffer.hasRemaining()) {
						int nodeId = ioBuffer.getInt();
						ArrayList<Integer> neighbors = new ArrayList<Integer>();
						int neighborIdSize = ioBuffer.getInt();
						for (int i = 0; i < neighborIdSize; ++i) {
							neighbors.add(ioBuffer.getInt());
						}
						logger.info("parsing large_vertex: " + nodeId);
						outsidePartitionedVertices.put(nodeId, neighbors);
					}
					getMachineStats().updateStatForSuperstep(
						StatName.DATA_PARSER_TIME_SPENT_ON_LARGE_VERTEX_DATA_MESSAGES, superstepNo,
						(double) (System.currentTimeMillis() - timeBefore));
					controlMessagesStats.addGlobalControlMessage(
						ControlMessageType.RECEIVED_LARGE_VERTEX_PARTITIONING_MESSAGES,
						fromMachineId);
					break;
				case INITIAL_VERTEX_PARTITIONING:
					ioBuffer = incomingBufferedDataMessage.getIoBuffer();
					MinaWritable tmpVertexValue = null;
					while (ioBuffer.hasRemaining()) {
						int vertexId = ioBuffer.getInt();
						
						if (representativeVertexValueClassForInputParsing != null) {
							tmpVertexValue =
								representativeVertexValueClassForInputParsing.newInstance();
							tmpVertexValue.read(ioBuffer);
						} else {
							tmpVertexValue = representativeVertex.getInitialValue(vertexId);
						}
						int numNeighbors = ioBuffer.getInt();
						graphPartition.put(vertexId, tmpVertexValue);
						for (int i = 0; i < numNeighbors; ++i) {
							if (!(representativeEdgeValue instanceof NullWritable)) {
								graphPartition.addEdge(vertexId, ioBuffer.getInt(), ioBuffer);
							} else {
								graphPartition.addEdge(vertexId, ioBuffer.getInt());
							}
						}
					}
					break;
				case FINAL_DATA_SENT:
					logger.debug("Incrementing SENT_FINAL_DATA message machineId: "
						+ incomingBufferedDataMessage.getFromMachineId() + " superstepNo: "
						+ superstepNo);
					controlMessagesStats.addPerSuperstepControlMessage(superstepNo,
						ControlMessageType.RECEIVED_FINAL_DATA_SENT_MESSAGES,
						incomingBufferedDataMessage.getFromMachineId());
					if (controlMessagesStats.hasReceivedAllPerSuperstepControlMessages(superstepNo,
						ControlMessageType.RECEIVED_FINAL_DATA_SENT_MESSAGES,
						GPSWorkerExposedGlobalVariables.getNumWorkers())) {
						getMachineStats().updateStatForSuperstep(
							StatName.DATA_PARSER_LAST_MESSAGE_PARSING_TIME, superstepNo,
							(double) System.currentTimeMillis());
					}
					break;
				case FINISHED_PARSING_DATA_MESSAGES:
					controlMessagesStats.addPerSuperstepControlMessage(superstepNo,
						ControlMessageType.RECEIVED_FINAL_INITIAL_VERTEX_PARTITIONING_MESSAGES,
						incomingBufferedDataMessage.getFromMachineId());
					break;
				case FINAL_INITIAL_VERTEX_PARTITIONING_SENT:
					controlMessagesStats.addGlobalControlMessage(
						ControlMessageType.RECEIVED_FINAL_INITIAL_VERTEX_PARTITIONING_MESSAGES,
						incomingBufferedDataMessage.getFromMachineId());
					break;
				default:
					parseIncomingBufferedMessage(incomingBufferedDataMessage, type, superstepNo);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void setIncomingMessageStorage(IncomingMessageStorage<MW> incomingMessageStorage) {
		this.incomingMessageStorage = incomingMessageStorage;
	}

	protected GlobalObjectsMap getLatestGlobalObjectsMap() {
		return null;
	}

	protected void parseIncomingBufferedMessage(IncomingBufferedMessage incomingBufferedDataMessage,
		MessageTypes type, int superstepNo) throws InstantiationException, IllegalAccessException {
		// Nothing to do
		logger.error("DataAndControlMessagesParser does not know how to parse a message of type: "
			+ type);
	}
}