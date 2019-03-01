package gps.messages.storage;

import java.util.List;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

/**
 * Base class to abstract the functionality of different message storers. We want to experiment with
 * the effects of different data structures for storing the incoming messages on the performance of
 * the system (e.g array based vs. linked-list based)
 * 
 * @author semihsalihoglu
 */
public abstract class IncomingMessageStorage<M extends MinaWritable> {

	public abstract Iterable<M> getMessageValuesForCurrentSuperstep(int nodeId);

	public abstract void addVertexToStorage(int localId, int numNeighbors);

	public abstract void addMessageToQueue(int toNodeId,
		IoBuffer ioBuffer);

	public abstract void addMessageToQueues(
		List<Integer> toNodeIds, IoBuffer ioBuffer);

	public abstract void startingSuperstep();

	public abstract void adjustArraySizesForNewVertex(int idOfVertexReceivedInPreviousSuperstep,
		int superspteNo);

	public abstract Iterable<M> getMessageValuesForNextSuperstep(int localId);

	public abstract void setNextSuperstepQueueMapAndIndices(int superstepNo);
}
