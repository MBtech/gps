package gps.examples.mst.edgesatrootpjonebyone;

import org.apache.commons.cli.CommandLine;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTMaster;
import gps.examples.mst.MSTOptions.EDGE_STORAGE_TECHNIQUE;
import gps.examples.mst.EdgeStorageAtRootMasterComputationStages;

public class EdgesAtRootPJOneByOneMaster extends MSTMaster {

	public EdgesAtRootPJOneByOneMaster(CommandLine commandLine) {
		super(commandLine);
		this.edgeStorageTechnique = EDGE_STORAGE_TECHNIQUE.AT_THE_ROOT;
	}
	
	@Override
	protected void doFurtherMasterComputation(ComputationStage computationStage) {
		if (!EdgeStorageAtRootMasterComputationStages.executeMasterComputationStage(computationStage, this)) {
			System.err.println("Could not execute compStage: " + computationStage + " in "
				+ this.getClass().getCanonicalName());
		}
	}
}
