package gps.examples.wcc.hybrid;

import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;

public class JobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
		return SingplePivotHybridWCCVertex.SingplePivotHybridWCCVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return SingplePivotHybridWCCVertex.class;
	}

	@Override
	public Class<?> getVertexValueClass() {
		return IntWritable.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
		return IntWritable.class;
	}

	public Class<?> getMasterClass() {
		return SinglePivotHybridWCCMaster.class;
	}
}