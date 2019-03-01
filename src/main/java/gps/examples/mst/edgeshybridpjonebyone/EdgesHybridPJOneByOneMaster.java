package gps.examples.mst.edgeshybridpjonebyone;

import org.apache.commons.cli.CommandLine;

import gps.examples.mst.EdgeStorageAtRootMasterComputationStages;
import gps.examples.mst.EdgeStorageAtSelfMasterComputationStages;
import gps.examples.mst.MSTOptions;
import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTOptions.EDGE_STORAGE_TECHNIQUE;
import gps.examples.mst.edgesatselfpjonebyone.EdgesAtSelfPJOneByOneMaster;
import gps.globalobjects.IntSumGlobalObject;

/**
 * Runs edge cleaning and min finding at self until the number of vertices that ha
 * @author semihsalihoglu
 */
public class EdgesHybridPJOneByOneMaster extends EdgesAtSelfPJOneByOneMaster {

	public EdgesHybridPJOneByOneMaster(CommandLine commandLine) {
		super(commandLine);
	}

	@Override
	protected void doFurtherMasterComputation(ComputationStage computationStage) {
		if (ComputationStage.NOTIFY_SUBVERTICES_OF_NEW_ROOT_2 == computationStage) {
				IntSumGlobalObject numSupernodesGO = (IntSumGlobalObject) getGlobalObjectsMap()
					.getGlobalObject(MSTOptions.GOBJ_NUM_SUPERNODES);
				if (numSupernodesGO != null) {
					this.numSupernodes = numSupernodesGO.getValue().getValue();
				}
				if (numSupernodes <  options.numSupernodesThresholdForSwitchingToEdgesAtRoot) {
					System.out.println("HybridMaster is switching to AT_THE_ROOT...");
					this.edgeStorageTechnique = EDGE_STORAGE_TECHNIQUE.AT_THE_ROOT;
					clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_ROOT_EDGE_CLEANING_1);
				} else {
					clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_SELF_EDGE_CLEANING_1);
				}
		} else {
			if (this.edgeStorageTechnique == EDGE_STORAGE_TECHNIQUE.AT_SELF) {
				if (!EdgeStorageAtSelfMasterComputationStages.executeMasterComputationStage(
					computationStage, this)) {
					System.err.println("Could not execute compStage: " + computationStage + " in "
						+ this.getClass().getCanonicalName());
				}
			} else {
				if (!EdgeStorageAtRootMasterComputationStages.executeMasterComputationStage(
					computationStage, this)) {
					System.err.println("Could not execute compStage: " + computationStage + " in "
						+ this.getClass().getCanonicalName());
				}
			}
		}
	}
}