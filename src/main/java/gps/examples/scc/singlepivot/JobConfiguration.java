package gps.examples.scc.singlepivot;

import gps.examples.scc.SCCBaseJobConfiguration;

/**
 * JobConfiguration class for the only single-pivot algorithm.
 * Warning: This algorithm should not be run on large graphs as it find only a single SCC per iteration and
 * will not converge in a reasonable amount of time.
 *
 * @author semihsalihoglu
 */
public class JobConfiguration extends SCCBaseJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
		return SinglePivotVertex.VertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
		return SinglePivotVertex.class;
	}

	@Override
	public Class<?> getMasterClass() {
		return SinglePivotMaster.class;
	}
}