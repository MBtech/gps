package gps.examples.scc.coloring;

import gps.examples.scc.SCCBaseJobConfiguration;

/**
 * JobConfiguration for the Single-Pivot/Coloring hybrid algorithm.
 *
 * @author semihsalihoglu
 */
public class JobConfiguration extends SCCBaseJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
		return ColoringVertex.VertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return ColoringVertex.class;
	}

	@Override
	public Class<?> getMasterClass() {
		return ColoringMaster.class;
	}
}