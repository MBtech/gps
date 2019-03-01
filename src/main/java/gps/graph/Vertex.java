package gps.graph;

import java.util.Random;

import gps.globalobjects.GlobalObjectsMap;
import gps.node.MinaWritableIterable.MessagesIterator;
import gps.node.worker.GPSWorkerExposedGlobalVariables;
import gps.node.worker.MessageSender;
import gps.writable.MinaWritable;

/**
 * Base class to represent the computation done by each vertex as well as vertex, edge and
 * message types.
 * 
 * @author semihsalihoglu
 *
 * @param <V>: {@link MinaWritable} vertex type
 * @param <E>: {@link MinaWritable} edge type
 * @param <M>: {@link MinaWritable} message type
 */
public abstract class Vertex<V extends MinaWritable, E extends MinaWritable,
	M extends MinaWritable> {

	public static byte ACTIVE_AS_BYTE = (byte) 1;
	public static byte INACTIVE_AS_BYTE = (byte) 0;

	public static boolean ACTIVE = true;
	public static boolean INACTIVE = false;

	public static MessageSender messageSender = null;
	public static Graph graphPartition = null;
	public static GlobalObjectsMap globalObjectsMap = null;
	public int localId;
	public static Random random;
	public M sentMessage;
	// Whether to intercept message for LALP optimization.
	public static boolean interceptMessage = false;

	public void sendMessages(int[] neighborIdsArray, M messageValue) {
		if (interceptMessage) {
			this.sentMessage = messageValue;
			return;
		}
		for (int neighborId : neighborIdsArray) {
			sendMessage(neighborId, messageValue);
		}
	}
	
	public void sendMessage(int neighborId, M messageValue) {
		messageSender.sendDataMessage(messageValue, neighborId);
	}
	
	public void voteToHalt() {
		graphPartition.setIsActiveOfLocalId(localId, false);
	}

	public V getValue() {
		// TODO(semih): Ask around about how to do this without a cast
		return (V) graphPartition.getValueOfLocalId(localId);
	}

	public int[] getNeighborIds() {
		return graphPartition.getNeighborIdsOfLocalId(localId);
	}
	
	public Iterable<Edge<E>> getOutgoingEdges() {
		return graphPartition.getOutgoingEdgesOfLocalId(localId);
	}

	public void removeEdges() {
		graphPartition.removeEdgesOfLocalId(localId);
	}
	
	public void addEdges(int[] neighborIds, E[] edgeValues) {
		graphPartition.addEdges(getId(), neighborIds, edgeValues);
	}
	
	public void addEdge(int neighborId, E edgeValue) {
		graphPartition.addEdge(getId(), neighborId, edgeValue);
	}

	public void setValue(V value) {
		graphPartition.setValueOfLocalId(localId, value);
	}

	public void relabelIdOfNeighbor(int neighborIdIndex, int newId) {
		graphPartition.relabelIdOfLocalId(localId, neighborIdIndex, newId);
	}
	public int getGraphSize() {
		return GPSWorkerExposedGlobalVariables.getGraphSize();
	}
	
	public int getNeighborsSize() {
		return graphPartition.getNeighborsSize(localId);
	}
	
	public GlobalObjectsMap getGlobalObjectsMap() {
		return globalObjectsMap;
	}

	// Warning: This should only be called by AbstractGPSWorker.
	public void setLocalId(int localId) {
		this.localId = localId;
	}
	
	public int getId() {
		return graphPartition.getGlobalId(localId);
	}

	public abstract void compute(Iterable<M> messageValues, int superstepNo);

	public V getInitialValue(int id) {
		return null;
	}

	public void resetMessagesToBeginning(Iterable<M> messageValues) {
		((MessagesIterator<M>) messageValues.iterator()).reset();
	}

	public void doWorkBeforeSuperstepComputation() {
		// Nothing to do.
	}

	public Random getRandom() {
		return random;
	}
	
	public void printToStdErrAndThrowARuntimeException(String errorString) {
		System.err.println(errorString);
		throw new RuntimeException(errorString);
	}
	
	public void logVertex() {
		System.out.println("id: " + getId() + " value: " + getValue());
		for (Edge<E> edge : getOutgoingEdges()) {
			System.out.print(" " + edge.getNeighborId() + " value: " + edge.getEdgeValue().toString());
		}
		System.out.println();
	}
}
