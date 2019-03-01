package gps.graph;

import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

import cern.colt.map.OpenIntIntHashMap;

/**
 * Graph interface that hold vertices of type V, and edges of type E.
 * 
 * @author semihsalihoglu
 *
 * @param <V> vertex value types
 * @param <E> edge value types
 */
public interface Graph<V extends MinaWritable, E extends MinaWritable> {

	public int getLocalId(int id);

	public int getGlobalId(int id);
	
	public boolean contains(int id);

	public int[] getNeighborIdsOfLocalId(int localId);

	public Iterable<Edge<E>> getOutgoingEdgesOfLocalId(int localId);
	
	public void setValueOfLocalId(int localId, V value);

	public int getOriginalIdOfLocalId(int localId);

	public V getValueOfLocalId(int localId);

	public void setIsActiveOfLocalId(int localId, boolean isActive);

	public boolean isActiveOfLocalId(int localId);
	
	public int put(int id, V value);

	public void relabelIds(OpenIntIntHashMap relabelsMap);

	public void put(int id, int originalId, V value, int[] neighborIds, boolean isActive,
		boolean isReplacement);

	public void addEdge(int id, int neighborId);

	public void addEdge(int id, int neighborId, IoBuffer ioBuffer);

	public void addEdge(int id, int neighborId, E edgeValue);

	public void addEdges(int id, int[] neighborIds, E[] edgeValues);

	public int size();

	public void finishedParsingGraph();

	public void dumpGraphPartition();

	public void setExeptionNeighborSizes(Map<Integer, Integer> neighborSizes);

	public int getNeighborsSize(int localId);

	public void removeEdgesOfLocalId(int localId);

	public int getNumEdges();

	void relabelIdOfLocalId(int localId, int neighborIdIndex, int newId);
}