package gps.examples.mst.edgesatselfpjonebyone;

import gps.examples.mst.EdgeStorageAtSelfVertexComputationStages;
import gps.examples.mst.MSTEdgeValue;
import gps.examples.mst.MSTMessageValue;
import gps.examples.mst.MSTVertex;
import gps.examples.mst.MSTVertexValue;
import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.graph.Vertex;
import gps.graph.VertexFactory;

import org.apache.commons.cli.CommandLine;

public class EdgeStorageAtSelfPointerJumpingOneByOneMSTVertex extends MSTVertex {

	public EdgeStorageAtSelfPointerJumpingOneByOneMSTVertex(CommandLine commandLine) {
		super(commandLine);
	}

	@Override
	protected void computeFurther(Iterable<MSTMessageValue> messageValues,
		ComputationStage computationStage) {
		if (!EdgeStorageAtSelfVertexComputationStages.executeComputationStage(this, messageValues,
			computationStage)) {
			System.out.println("ERROR!!! Could not execute computation stage for" +
				this.getClass().getName());
		}
	}

	public static class EdgeStorageAtSelfPointerJumpingOneByOneMSTVertexFactory extends
		VertexFactory<MSTVertexValue, MSTEdgeValue, MSTMessageValue> {

		@Override
		public Vertex<MSTVertexValue, MSTEdgeValue, MSTMessageValue> newInstance(CommandLine commandLine) {
			return new EdgeStorageAtSelfPointerJumpingOneByOneMSTVertex(commandLine);
		}
	}
}