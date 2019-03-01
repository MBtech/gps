package gps.examples.scc.hybrid;

import gps.examples.scc.SCCBaseJobConfiguration;

/**
 * JobConfiguration for the Single-Pivot/Coloring hybrid algorithm.
 *
 * @author semihsalihoglu
 */
public class JobConfiguration extends SCCBaseJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
		return HybridSCCVertex.VertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return HybridSCCVertex.class;
	}

	@Override
	public Class<?> getMasterClass() {
		return HybridSCCMaster.class;
	}
}