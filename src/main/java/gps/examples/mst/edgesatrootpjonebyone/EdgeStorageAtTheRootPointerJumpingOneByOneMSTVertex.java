package gps.examples.mst.edgesatrootpjonebyone;

import gps.examples.mst.EdgeStorageAtRootVertexComputationStages;
import gps.examples.mst.MSTEdgeValue;
import gps.examples.mst.MSTMessageValue;
import gps.examples.mst.MSTVertex;
import gps.examples.mst.MSTVertexValue;
import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.graph.Vertex;
import gps.graph.VertexFactory;

import org.apache.commons.cli.CommandLine;

public class EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertex extends MSTVertex {

	public EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertex(CommandLine commandLine) {
		super(commandLine);
	}

	@Override
	protected void computeFurther(Iterable<MSTMessageValue> messageValues,
		ComputationStage computationStage) {
		if (!EdgeStorageAtRootVertexComputationStages.executeComputationStage(this,
			messageValues, computationStage)) {
			System.out.println("ERROR!!! Could not execute computation stage for" +
				this.getClass().getName());
		}
	}

	public static class EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertexFactory extends
		VertexFactory<MSTVertexValue, MSTEdgeValue, MSTMessageValue> {

		@Override
		public Vertex<MSTVertexValue, MSTEdgeValue, MSTMessageValue> newInstance(CommandLine commandLine) {
			return new EdgeStorageAtTheRootPointerJumpingOneByOneMSTVertex(commandLine);
		}
	}
}