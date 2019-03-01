package gps.examples.mst.edgeshybridpjonebyone;

import gps.examples.mst.EdgeStorageAtSelfVertexComputationStages;
import gps.examples.mst.EdgeStorageAtRootVertexComputationStages;
import gps.examples.mst.MSTEdgeValue;
import gps.examples.mst.MSTMessageValue;
import gps.examples.mst.MSTVertex;
import gps.examples.mst.MSTVertexValue;
import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.graph.Vertex;
import gps.graph.VertexFactory;

import org.apache.commons.cli.CommandLine;

public class EdgeStorageHybridPJOneByOneVertex extends MSTVertex {

	public EdgeStorageHybridPJOneByOneVertex(CommandLine commandLine) {
		super(commandLine);
		options.useHashMapInEdgeCleaningForEdgesAtRoot = true;
	}

	@Override
	protected void computeFurther(Iterable<MSTMessageValue> messageValues,
		ComputationStage computationStage) {
		if (EdgeStorageAtRootVertexComputationStages.executeComputationStage(this,
			messageValues, computationStage)) {
			return;
		} else if (EdgeStorageAtSelfVertexComputationStages.executeComputationStage(this,
			messageValues, computationStage)) {
			return;
		}
		System.out.println("ERROR!!! Could not execute computation stage for " +
			this.getClass().getName());
	}

	public static class EdgeStorageHybridPointerJumpingOneByOneMSTVertexFactory extends
		VertexFactory<MSTVertexValue, MSTEdgeValue, MSTMessageValue> {

		@Override
		public Vertex<MSTVertexValue, MSTEdgeValue, MSTMessageValue> newInstance(CommandLine commandLine) {
			return new EdgeStorageHybridPJOneByOneVertex(commandLine);
		}
	}
}