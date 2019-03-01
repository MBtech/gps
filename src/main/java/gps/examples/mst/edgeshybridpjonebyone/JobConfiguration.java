package gps.examples.mst.edgeshybridpjonebyone;

import gps.examples.mst.MSTJobConfiguration;
import gps.examples.mst.edgeshybridpjonebyone.EdgeStorageHybridPJOneByOneVertex.EdgeStorageHybridPointerJumpingOneByOneMSTVertexFactory;

public class JobConfiguration extends MSTJobConfiguration {

	@Override
	public Class<?> getMasterClass() {
		return EdgesHybridPJOneByOneMaster.class;
	}

	@Override
	public Class<?> getVertexFactoryClass() {
		return EdgeStorageHybridPointerJumpingOneByOneMSTVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return EdgeStorageHybridPJOneByOneVertex.class;
	}
}