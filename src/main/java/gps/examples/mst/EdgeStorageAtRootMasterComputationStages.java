package gps.examples.mst;

import gps.examples.mst.MSTComputationStage.ComputationStage;

/**
 * Contains the logic for the different master computation phases when the edges 
 * are always stored at the supernodes. The alternative is storing the edges always at self.
 * 
 * @author semihsalihoglu
 */
public class EdgeStorageAtRootMasterComputationStages {

	public static boolean executeMasterComputationStage(ComputationStage compStage, MSTMaster master) {
		switch (compStage) {
		case AT_ROOT_EDGE_CLEANING_1:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_ROOT_EDGE_CLEANING_2);
			return true;
		case AT_ROOT_EDGE_CLEANING_2:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_ROOT_EDGE_CLEANING_3);
			return true;
		case AT_ROOT_EDGE_CLEANING_3:
			if (!master.terminateIfNumActiveVerticesIsZero()) {
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.IMMEDIATE_MIN_EDGE_PICKING);
			}
			return true;
		default:
			return false;
		}
	}
}