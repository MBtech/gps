package gps.examples.mst.edgesatselfpjonebyone;

import org.apache.commons.cli.CommandLine;

import gps.examples.mst.EdgeStorageAtSelfMasterComputationStages;
import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTMaster;
import gps.examples.mst.MSTOptions.EDGE_STORAGE_TECHNIQUE;

public class EdgesAtSelfPJOneByOneMaster extends MSTMaster {

	public EdgesAtSelfPJOneByOneMaster(CommandLine commandLine) {
		super(commandLine);
		this.edgeStorageTechnique = EDGE_STORAGE_TECHNIQUE.AT_SELF;
	}

	@Override
	protected void doFurtherMasterComputation(ComputationStage computationStage) {
		if (!EdgeStorageAtSelfMasterComputationStages.executeMasterComputationStage(computationStage, this)) {
			System.err.println("Could not execute compStage: " + computationStage + " in "
				+ this.getClass().getCanonicalName());
		}
	}
}