package gps.examples.mis;

import gps.node.GPSJobConfiguration;

public class JobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
		return MISVertex.MISVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return MISVertex.class;
	}

	@Override
	public Class<?> getVertexValueClass() {
		return MISVertexValue.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
		return MISMessage.class;
	}
}