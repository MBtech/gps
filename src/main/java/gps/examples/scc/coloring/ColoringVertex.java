package gps.examples.scc.coloring;

import org.apache.commons.cli.CommandLine;

import gps.examples.scc.SCCVertexValue;
import gps.examples.scc.SCCMessageValue;
import gps.examples.scc.SCCBaseVertex;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;

/**
 * Vertex class for the Coloring algorithm from Orzan's thesis:
 * (https://www.cs.vu.nl/en/Images/SM%20Orzan%205-11-2004_tcm75-258582.pdf)
 *
 * @author semihsalihoglu
 */
public class ColoringVertex extends SCCBaseVertex {

	public ColoringVertex(CommandLine commandLine) {
		super(commandLine);
		this.isColoring = true;
	}

	public static class VertexFactory extends
		NullEdgeVertexFactory<SCCVertexValue, SCCMessageValue> {

		@Override
		public NullEdgeVertex<SCCVertexValue, SCCMessageValue> newInstance(CommandLine commandLine) {
			return new ColoringVertex(commandLine);
		}
	}
}