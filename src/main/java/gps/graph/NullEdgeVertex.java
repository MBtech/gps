package gps.graph;

import gps.writable.MinaWritable;
import gps.writable.NullWritable;

/**
 * Vertex class for graphs without any egde values.
 * 
 * @author semihsalihoglu
 */
public abstract class NullEdgeVertex<V extends MinaWritable, M extends MinaWritable> extends
	Vertex<V, NullWritable, M>{
}
