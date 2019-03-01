package gps.examples.mst.edgesatselfpjonebyone;

import gps.examples.mst.MSTJobConfiguration;
import gps.examples.mst.edgesatselfpjonebyone.EdgeStorageAtSelfPointerJumpingOneByOneMSTVertex.EdgeStorageAtSelfPointerJumpingOneByOneMSTVertexFactory;

public class JobConfiguration extends MSTJobConfiguration {

	@Override
	public Class<?> getMasterClass() {
		return EdgesAtSelfPJOneByOneMaster.class;
	}

	@Override
	public Class<?> getVertexFactoryClass() {
		return EdgeStorageAtSelfPointerJumpingOneByOneMSTVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return EdgeStorageAtSelfPointerJumpingOneByOneMSTVertex.class;
	}
}