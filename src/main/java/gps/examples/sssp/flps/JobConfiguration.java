package gps.examples.sssp.flps;

import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;
import gps.writable.BooleanWritable;

public class JobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
		return SSSPFLPSVertex.SSSPFLPSVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return SSSPFLPSVertex.class;
	}

	@Override
	public Class<?> getVertexValueClass() {
		return IntWritable.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
		return BooleanWritable.class;
	}

	public Class<?> getMasterClass() {
		return SSSPFLPSMaster.class;
	}
}