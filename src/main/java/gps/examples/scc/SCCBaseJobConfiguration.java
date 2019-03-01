package gps.examples.scc;

import gps.node.GPSJobConfiguration;

/**
 * Base class for different JobConfiguration classes for different SCC algorithms.
 * 
 * @author semihsalihoglu
 */
public abstract class SCCBaseJobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexValueClass() {
		return SCCVertexValue.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
		return SCCMessageValue.class;
	}
}