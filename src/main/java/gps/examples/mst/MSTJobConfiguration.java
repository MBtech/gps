package gps.examples.mst;

import gps.examples.mst.MSTEdgeValue;
import gps.examples.mst.MSTMessageValue;
import gps.examples.mst.MSTVertexValue;
import gps.node.GPSJobConfiguration;

public abstract class MSTJobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexValueClass() {
		return MSTVertexValue.class;
	}

	public Class<?> getEdgeValueClass() {
		return MSTEdgeValue.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
		return MSTMessageValue.class;
	}
}