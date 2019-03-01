package gps.graph;

import gps.writable.MinaWritable;

public class Edge<E extends MinaWritable> {

	private E edgeValue;
	private int neighborId;
	
	public int getNeighborId() {
		return neighborId;
	}
	
	public void setNeighborId(int neighborId) {
		this.neighborId = neighborId;
	}

	public E getEdgeValue() {
		return edgeValue;
	}

	public void setEdgeValue(E edgeValue) {
		this.edgeValue = edgeValue;
	}
}
