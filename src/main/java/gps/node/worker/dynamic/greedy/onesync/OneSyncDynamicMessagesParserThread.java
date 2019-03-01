package gps.node.worker.dynamic.greedy.onesync;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

import gps.graph.Graph;
import gps.graph.NullEdgeVertex;
import gps.graph.Vertex;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.storage.IncomingMessageStorage;
import gps.node.ControlMessagesStats;
import gps.node.GPSJobConfiguration;
import gps.node.worker.DataAndControlMessagesParserThread;
import gps.node.worker.dynamic.VertexWrapper;
import gps.writable.MinaWritable;

public class OneSyncDynamicMessagesParserThread<VW extends MinaWritable, MW extends MinaWritable>
	extends DataAndControlMessagesParserThread<MW> {

	private static Logger logger = Logger.getLogger(OneSyncDynamicMessagesParserThread.class);
	public static int numVertexDataReceived;
	private final ReceivedVertexDataWrapper<VW> receivedVertexDataWrapper;
	private final Class<VW> vertexRepresentativeClass;

	public OneSyncDynamicMessagesParserThread(IncomingMessageStorage<MW> incomingMessageStorage,
		BlockingQueue<IncomingBufferedMessage> incomingBufferedMessages,
		ControlMessagesStats controlMessagesStats,
		ReceivedVertexDataWrapper<VW> receivedVertexDataWrapper,
		Class<VW> vertexRepresentativeInstance, Class<MW> messageRepresentativeInstance,
		Graph graphPartition, Vertex vertex, MinaWritable representativeEdgeValue,
		GPSJobConfiguration jobConfiguration, boolean isNoDataParsing) throws InstantiationException, IllegalAccessException {
		super(incomingMessageStorage, incomingBufferedMessages, controlMessagesStats,
			messageRepresentativeInstance, graphPartition, vertex, 
			representativeEdgeValue, jobConfiguration, isNoDataParsing);
		this.receivedVertexDataWrapper = receivedVertexDataWrapper;
		this.vertexRepresentativeClass = vertexRepresentativeInstance;
	}

	@Override
	protected void parseIncomingBufferedMessage(
		IncomingBufferedMessage incomingBufferedDataMessage, MessageTypes type, int superstepNo)
		throws InstantiationException, IllegalAccessException {
		switch (type) {
		case EXCEPTIONS_MAP:
			logger.info("Not Parsing the exceptions file map. Only adding it to the exceptions" +
				" file buffer.");
			receivedVertexDataWrapper.exceptionFilesAndPotentialVerticesToSendMessages.add(
				incomingBufferedDataMessage);				
			break;
		case POTENTIAL_NUM_VERTICES_TO_SEND:
			receivedVertexDataWrapper.exceptionFilesAndPotentialVerticesToSendMessages.add(
				incomingBufferedDataMessage);
			break;
		case VERTEX_SHUFFLING_WITH_DATA:
			logger.info("Parsing Vertex Shuffling With Data.");
			IoBuffer ioBuffer = incomingBufferedDataMessage.getIoBuffer();
			while(ioBuffer.hasRemaining()) {
				numVertexDataReceived++;
				int vertexId = ioBuffer.getInt();
				VertexWrapper<VW> vertexWrapper = new VertexWrapper<VW>();
				vertexWrapper.originalId = ioBuffer.getInt();
				VW vertexValue = vertexRepresentativeClass.newInstance();
				vertexValue.read(ioBuffer);
				vertexWrapper.state = vertexValue;
				vertexWrapper.isActive =
					ioBuffer.get() == NullEdgeVertex.ACTIVE_AS_BYTE ? NullEdgeVertex.ACTIVE : NullEdgeVertex.INACTIVE;
				int numNeighbors = ioBuffer.getInt();
				int[] neighborIds = new int[numNeighbors];
				logger.debug("data for vertexId: " + vertexId + " originalVertexId: "
					+ vertexWrapper.originalId + " state: " + vertexWrapper.state
					+ " isActive: " + vertexWrapper.isActive + " numNeighbors: " + numNeighbors);
				for (int i = 0; i < numNeighbors; ++i) {
					neighborIds[i] = ioBuffer.getInt();
				}
				vertexWrapper.neighborIds = neighborIds;
				receivedVertexDataWrapper.verticesReceived.put(vertexId, vertexWrapper);
			}
			break;
		default:
			logger.error(this.getClass().getName() +
				" does not support parsing of messages of type: " + type);
		}
	}
}