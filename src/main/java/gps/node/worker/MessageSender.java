package gps.node.worker;

import java.util.List;

import gps.communication.MessageSenderAndReceiverForWorker;
import gps.writable.MinaWritable;

/**
 * This is a class that's plugged in between a {@link Vertex} object and implementations of the
 * {@link AbstractGPSWorker} class. Its goal is to intercept messages being sent by each vertex and
 * lay it over to the appropriate {@link MessageSenderAndReceiverForWorker} classes under the
 * {@link gps.communication} package. A good use case is for the greedily dynamically partitioning
 * implementations of {@link AbstractGPSWorker} class, the destination machine of each message being
 * sent has to be intercepted. For statically partitioning implementations of
 * {@link AbstractGPSWorker}, this class should just pass the message to the
 * {@link MessageSenderAndReceiverForWorker}.
 * 
 * @author semihsalihoglu
 */
public interface MessageSender {

	public void sendLargeVertexDataMessage(MinaWritable messageValue, int toNodeId,
		int superstepNo);

	public void sendDataMessage(MinaWritable messageValue, int toNodeId);

	public void sendVertex(int vertexId, MinaWritable vertexValue, List<Integer> neighborIds,
		List<MinaWritable> edgeValues);

	public void sendDataMessageForLargeVertexToAllNeighbors(MinaWritable messageValue,
		int fromVertexId, int superstepNo);
}
