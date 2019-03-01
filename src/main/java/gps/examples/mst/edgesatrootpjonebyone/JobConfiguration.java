package gps.examples.mst.edgesatrootpjonebyone;

import gps.examples.mst.MSTJobConfiguration;
import gps.examples.mst.edgesatrootpjonebyone.EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertex.EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertexFactory;

public class JobConfiguration extends MSTJobConfiguration {

	@Override
	public Class<?> getMasterClass() {
		return EdgesAtRootPJOneByOneMaster.class;
	}

	@Override
	public Class<?> getVertexFactoryClass() {
		return EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertex.class;
	}
}