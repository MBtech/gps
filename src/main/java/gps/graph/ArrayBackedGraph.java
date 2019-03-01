package gps.graph;

import gps.writable.MinaWritable;
import gps.writable.NullWritable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

import cern.colt.map.OpenIntIntHashMap;

// Note 1: This class is not thread-safe. Different Implementations of dynamism may require this
// to be thread-safe. Implement thread-safety in that case.
// Note 2: If a vertex did not have its own line (i.e the line containing its adjacency list), we
// assume that it has incoming edges and still add it as a valid vertex which starts active.
// In essense we fill in any id gaps in the original input file.
public class ArrayBackedGraph<V extends MinaWritable, E extends MinaWritable>
	implements Graph<V, E> {

	private static Logger logger = Logger.getLogger(ArrayBackedGraph.class);

	private int[][] neighborIds = null;
	private int[] neighborIdIndices = null;
	private V[] valuesArray = null;
	private boolean[] isActiveArray = null;
	private int[] originalIdsArray = null;
	private final int numMachines;
	private int maxLocalId = -1;
	private final int localMachineId;
	private int numEdges = 0;
	private Class<V> vertexClass;
	private Map<Integer, Integer> exceptionNeighborSizes = new HashMap<Integer, Integer>();
	private final Vertex vertex;
	private byte[][][] edgeValueBytes;
	public EdgeIterable<E> outgoingEdges = new EdgeIterable<E>();

	private final E representativeEdgeValue;

	@SuppressWarnings("unchecked")
	public ArrayBackedGraph(int localMachineId, int numMachines, Class<V> vertexClass,
		Vertex  vertex, Class<E> representativeEdgeValueClass)
		throws InstantiationException, IllegalAccessException {
		this.localMachineId = localMachineId;
		this.numMachines = numMachines;
		this.representativeEdgeValue = representativeEdgeValueClass.newInstance();
		this.neighborIds = new int[1][];
		this.neighborIdIndices = new int[1];
		this.originalIdsArray = new int[1];
		this.edgeValueBytes = !(representativeEdgeValue instanceof NullWritable) ? new byte[1][][] : null;
		this.vertexClass = vertexClass;
		this.valuesArray = (V[]) Array.newInstance(vertexClass, 1);
		this.isActiveArray = new boolean[1];
		this.vertex = vertex;
		this.outgoingEdges.edgeIterator.setRepresentativeWritableInstance(
			representativeEdgeValueClass.newInstance());
	}

	public int getLocalId(int id) {
		return (id - localMachineId) / numMachines;
	}

	public int getGlobalId(int id) {
		return (id * numMachines) + localMachineId;
	}

	@Override
	public int[] getNeighborIdsOfLocalId(int localId) {
		return neighborIds[localId];
	}

	@Override
	public Iterable<Edge<E>> getOutgoingEdgesOfLocalId(int localId) {
		outgoingEdges.edgeIterator.init(edgeValueBytes[localId], getNeighborIdsOfLocalId(localId),
			neighborIdIndices[localId]);
		return outgoingEdges;
	}

	@Override
	public void setValueOfLocalId(int localId, V value) {
		// TODO(semih): This is likely to throw an Exception because
		// the array size is likely to not be of size localId
		valuesArray[localId] = value;
	}

	@Override
	public V getValueOfLocalId(int localId) {
		return valuesArray[localId];
	}

	@Override
	public void setIsActiveOfLocalId(int localId, boolean isActive) {
		isActiveArray[localId] = isActive;
	}

	@Override
	public boolean isActiveOfLocalId(int localId) {
		return isActiveArray[localId];
	}

	@Override
	public void addEdge(int id, int neighborId) {
		addEdge(id, neighborId, null, null);
	}

	@Override
	public void addEdge(int id, int neighborId, IoBuffer ioBuffer) {
		addEdge(id, neighborId, ioBuffer, null);
	}

	@Override
	public void addEdge(int id, int neighborId, E edgeValue) {
		addEdge(id, neighborId, null, edgeValue);
	}

	@Override
	public void addEdges(int id, int[] neighborIdsToAdd, E[] edgeValuesToAdd) {
		if (edgeValuesToAdd != null && neighborIdsToAdd.length != edgeValuesToAdd.length) {
			throw new IllegalArgumentException("ERROR!!! When adding multiple edges to the graph with values," +
				" neighborIdsToAdd.length has to equal edgeValuesToAdd.length");
		}
		for (int i = 0; i < neighborIdsToAdd.length; ++i) {
			addEdge(id, neighborIdsToAdd[i], null, edgeValuesToAdd == null ? null : edgeValuesToAdd[i]);
		}
	}

	private void addEdge(int id, int neighborId, IoBuffer ioBuffer, E edgeValueObject) {
		if (id == neighborId) {
			if (edgeValueObject != null || !(representativeEdgeValue instanceof NullWritable)) {
				representativeEdgeValue.read(ioBuffer, new byte[representativeEdgeValue.numBytes()], 0);
			}
			return;
		}
		int localId = getLocalId(id);
		int neighborIdIndex = neighborIdIndices[localId];
		int[] neighbors = neighborIds[localId];
		byte[][] edgeValues = null;
		if (edgeValueObject != null || !(representativeEdgeValue instanceof NullWritable)) {
			edgeValues = edgeValueBytes[localId];
		}
		if (neighborIdIndex == neighbors.length) {
			int newNeighborIdsArrayLength =
				Math.max(1, neighborIdIndex > 1000 ? (int) (neighborIdIndex * 1.2) : neighborIdIndex * 2);
			int[] newNeighborIds = new int[newNeighborIdsArrayLength];
			System.arraycopy(neighbors, 0, newNeighborIds, 0, neighbors.length);
			for (int i = 0; i < neighborIdIndex; ++i) {
				newNeighborIds[i] = neighbors[i];
			}
			neighborIds[localId] = newNeighborIds;
			neighbors = newNeighborIds;
			if (edgeValues != null) {
				byte[][] newEdgeValues = new byte[newNeighborIdsArrayLength][];
				System.arraycopy(edgeValues, 0, newEdgeValues, 0, edgeValues.length);
				edgeValueBytes[localId] = newEdgeValues;
				edgeValues = newEdgeValues;
			}
		}
		neighbors[neighborIdIndex] = neighborId;
		neighborIdIndices[localId] += 1;
		if (edgeValues != null) {
			byte[] newEdgeValue;
			if (edgeValueObject != null) {
				newEdgeValue = edgeValueObject.getByteArray();
			} else {
				newEdgeValue = new byte[representativeEdgeValue.numBytes()];
				representativeEdgeValue.read(ioBuffer, newEdgeValue, 0);
			}
			edgeValues[neighborIdIndex] = newEdgeValue;
		}
		numEdges++;
	}

	@Override
	public int put(int id, V value) {
		int localId = getLocalId(id); // nextVertexIndex;
		if (localId < 0) {
			logger.error("localId is negative: " + localId + " id: " + id);
			return -1;
		}
		if (localId > maxLocalId) {
			maxLocalId = localId;
			resizeArraysIfNecessary(maxLocalId + 1);
		}
		neighborIds[localId] = new int[1];
		neighborIdIndices[localId] = 0;
		originalIdsArray[localId] = id;
		valuesArray[localId] = value;
		isActiveArray[localId] = true;
		if (!(representativeEdgeValue instanceof NullWritable)) {
			edgeValueBytes[localId] = new byte[1][];
		}		
		return localId;
	}

	private void resizeArraysIfNecessary(int maxLocalId) {
		if (maxLocalId >= valuesArray.length) {
			int newArrayLength = valuesArray.length > 1000000 ?
				Math.max(maxLocalId, (int) (valuesArray.length * 1.05))
					: Math.max(maxLocalId, valuesArray.length * 2);
			logger.info("Resizing ArrayBackedGraph to: " + newArrayLength + " maxLocalId: " + maxLocalId);
			resizeAllArrays(newArrayLength, false /* not shrinking */);
		}
	}

	private void resizeAllArrays(int newArrayLength, boolean shrinking) {
		int[][] newNeighborIds = new int[newArrayLength][];
		int[] newNeighborIdIndices = new int[newArrayLength];
		int[] newOriginalIdsArray = new int[newArrayLength];
		byte[][][] newEdgeValueBytes = null;
		if (!(representativeEdgeValue instanceof NullWritable)) {
			newEdgeValueBytes = new byte[newArrayLength][][];
		}
		@SuppressWarnings("unchecked")
		V[] newStatesArray =  (V[]) Array.newInstance(vertexClass, newArrayLength);
		boolean[] newIsActiveArray = new boolean[newArrayLength];
		int copyIndex = shrinking ? newArrayLength : valuesArray.length;
		for (int i = 0; i < copyIndex; ++i) {
			newNeighborIds[i] = neighborIds[i];
			newOriginalIdsArray[i] = originalIdsArray[i];
			newNeighborIdIndices[i] = neighborIdIndices[i];
			newStatesArray[i] = valuesArray[i];
			newIsActiveArray[i] = isActiveArray[i];
			if (newEdgeValueBytes != null) {
				newEdgeValueBytes[i] = edgeValueBytes[i];
			}
		}
		neighborIds = newNeighborIds;
		neighborIdIndices = newNeighborIdIndices;
		originalIdsArray = newOriginalIdsArray;
		valuesArray = newStatesArray;
		isActiveArray = newIsActiveArray;
		if (newEdgeValueBytes != null) {
			edgeValueBytes = newEdgeValueBytes;
		}
	}

	@Override
	public void relabelIds(OpenIntIntHashMap relabelsMap) {
		logger.debug("--------Logging Relabeling------------");
		int[] neighborIdsArray;
		int neighborId;
		for (int i = 0; i < neighborIds.length; ++i) {
			neighborIdsArray = neighborIds[i];
			for (int j = 0; j < neighborIdIndices[i]; ++j) {
				neighborId = neighborIdsArray[j];
				if (relabelsMap.containsKey(neighborId)) {
					int newNeighborId = relabelsMap.get(neighborId);
					logger.debug("neighborId of localId: " + i + " is being relabeled from: "
						+ neighborId  + " to: " + newNeighborId);
					neighborIdsArray[j] = newNeighborId;
				}
			}
		}
		logger.debug("--------End of logging Relabeling------------");
	}

	@Override
	public void relabelIdOfLocalId(int localId, int neighborIdIndex, int newId) {
		this.neighborIds[localId][neighborIdIndex] = newId;
	}
	
	@Override
	public int size() {
		return valuesArray.length; // idsMap.size() + reservedIdsMap.size();
	}

	@Override
	public void finishedParsingGraph() {
		logger.info("maxLocalId: " + maxLocalId);
		resizeAllArrays(maxLocalId + 1, true /* shrinking */);
		for (int i = 0; i < maxLocalId + 1; ++i) {
			if (neighborIds[i] == null) {
				V value = getValueOfLocalId(i) == null ? (V) vertex.getInitialValue(getGlobalId(i)):
					getValueOfLocalId(i);
				put(getGlobalId(i), getGlobalId(i), value,
					new int[0], isActiveOfLocalId(i), false /* not a replacement */);
				continue;
			}
			if (getValueOfLocalId(i) == null) {
				setValueOfLocalId(i, (V) vertex.getInitialValue(getGlobalId(i)));				
			}
			resizeEdgesForLocalId(i);
		}
	}

	private void resizeEdgesForLocalId(int i) {
		neighborIds[i] = Arrays.copyOf(neighborIds[i], neighborIdIndices[i]);
		if (edgeValueBytes != null && edgeValueBytes.length > i) {
			edgeValueBytes[i] = Arrays.copyOf(edgeValueBytes[i], neighborIdIndices[i]);
		}
	}

	@Override
	public boolean contains(int id) {
		int localId = getLocalId(id);
		return localId <= maxLocalId && localId >= 0 && ((id % numMachines) == localMachineId); // idsMap.containsKey(id) || reservedIdsMap.containsKey(id);
	}

	@Override
	public void put(int id, int originalId, V value, int[] neighbors, boolean isActive,
		boolean isReplacement) {
		int localId = getLocalId(id);
		if (isReplacement && neighborIds[localId] != null) {
			numEdges -= neighborIds[localId].length;
		}
		neighborIds[localId] = neighbors;
		neighborIdIndices[localId] = neighbors.length;
		originalIdsArray[localId] = originalId;
		valuesArray[localId] = value;
		isActiveArray[localId] = isActive;
		numEdges += neighbors.length;
	}

	@Override
	public int getOriginalIdOfLocalId(int localId) {
		return originalIdsArray[localId];
	}

	@Override
	public void removeEdgesOfLocalId(int localId) {
		numEdges -= this.neighborIdIndices[localId];
		this.neighborIds[localId] = new int[0];
		this.neighborIdIndices[localId] = 0;
		if (this.edgeValueBytes != null && this.edgeValueBytes[localId] != null) {
			this.edgeValueBytes[localId] = new byte[0][];
		}
	}

	@Override
	public void dumpGraphPartition() {
		logger.info("Start of dumping graph partition...");
		for (int localId = 0; localId < size(); ++localId) {
			String debugString = "" + getGlobalId(localId);
			if (!(representativeEdgeValue instanceof NullWritable) && edgeValueBytes[localId] != null) {
				for (Edge<E> edge : getOutgoingEdgesOfLocalId(localId)) {
					debugString += " " + edge.getNeighborId() + " " + edge.getEdgeValue();
				}
			} else {
				for (int neighborId : neighborIds[localId]) {
					debugString += " " + neighborId;
				}
			}
			debugString += " isActive: " + isActiveArray[localId];
			debugString += " value: " + valuesArray[localId];
			logger.info(debugString);
		}
		logger.info("End of dumping graph partition...");
	}

	@Override
	public int getNeighborsSize(int localId) {
		return exceptionNeighborSizes.containsKey(localId) ? exceptionNeighborSizes.get(localId)
			: neighborIdIndices[localId];
	}

	@Override
	public void setExeptionNeighborSizes(Map<Integer, Integer> exceptionNeighborSizes) {
		this.exceptionNeighborSizes = exceptionNeighborSizes;
	}

	@Override
	public int getNumEdges() {
		return numEdges;
	}
}