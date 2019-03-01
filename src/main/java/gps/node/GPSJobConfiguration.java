package gps.node;

import gps.graph.Master;
import gps.writable.NullWritable;

/**
 * Abstract class to configure GPS jobs. Stores values such as vertex class,
 * vertex factory class, vertex, edge and message value types.
 * 
 * @author semihsalihoglu
 */
public abstract class GPSJobConfiguration {

	public abstract Class<?> getVertexFactoryClass();
	public abstract Class<?> getVertexClass();
	public abstract Class<?> getVertexValueClass();

	public abstract Class<?> getMessageValueClass();

	public boolean hasVertexValuesInInput() {
		return false;
	}

	public Class<?> getMasterClass() {
		return Master.class;
	}

	public Class<?> getEdgeValueClass() {
		return NullWritable.class;
	}
	
	public String toString() {
		StringBuilder retVal = new StringBuilder();
		retVal.append("vertexFactoryClass: " + getVertexFactoryClass().getCanonicalName() + "\n");
		retVal.append("vertexClass: " + getVertexClass().getCanonicalName() + "\n");
		retVal.append("masterClass: " + getMasterClass().getCanonicalName() + "\n");
		retVal.append("vertexValueClass: " + getVertexValueClass().getCanonicalName() + "\n");
		retVal.append("messageValueClass: " + getMessageValueClass().getCanonicalName() + "\n");
		retVal.append("edgeValueClass: " + getEdgeValueClass().getCanonicalName() + "\n");
		return retVal.toString();
	}
}
