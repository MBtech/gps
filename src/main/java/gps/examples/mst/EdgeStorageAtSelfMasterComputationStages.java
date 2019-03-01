package gps.examples.mst;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.globalobjects.IntSumGlobalObject;

public class EdgeStorageAtSelfMasterComputationStages {

	public static boolean executeMasterComputationStage(ComputationStage compStage, MSTMaster master) {
		switch (compStage) {
		case MIN_EDGE_PICKING_1:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.MIN_EDGE_PICKING_2);
			return true;
		case MIN_EDGE_PICKING_2:
			if (!master.terminateIfNumActiveVerticesIsZero()) {
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.POINTER_JUMPING_QUESTION_1);
			}
			return true;
		case NOTIFY_NEW_ROOT_THAT_SELF_IS_A_SUBVERTEX:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.NOTIFY_SUBVERTICES_OF_NEW_ROOT_1);
			return true;
		case NOTIFY_SUBVERTICES_OF_NEW_ROOT_1:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.NOTIFY_SUBVERTICES_OF_NEW_ROOT_2);
			return true;
		case NOTIFY_SUBVERTICES_OF_NEW_ROOT_2:
			if (master.options.isECOD) {
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_MIN_EDGE_PICKING_QUESTION);
				master.latestECODIteration = 1;
			} else {
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_SELF_EDGE_CLEANING_1);
			}
			return true;
		case ECOD_MIN_EDGE_PICKING_QUESTION:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_MIN_EDGE_PICKING_ANSWER_SENDING);
			return true;
		case ECOD_MIN_EDGE_PICKING_ANSWER_SENDING:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_MIN_EDGE_PICKING_ANSWER_RECEIVING);
			return true;
		case ECOD_MIN_EDGE_PICKING_ANSWER_RECEIVING:
			if (master.latestECODIteration < master.options.numECODIterations) {
				master.latestECODIteration++;
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_MIN_EDGE_PICKING_QUESTION);
			} else {
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_EDGE_CLEANING_1);
			}
			return true;
		case ECOD_EDGE_CLEANING_1:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_EDGE_CLEANING_2);
			return true;
		case ECOD_EDGE_CLEANING_2:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.ECOD_EDGE_CLEANING_3);
			return true;
		case ECOD_EDGE_CLEANING_3:
			// We move to regular min_edge_picking at this point because now every subvertex can now
			// figure out its min weight edge.
			if (!master.terminateIfNumActiveVerticesIsZero()) {
				master.clearGlobalObjectsAndSetComputationStage(ComputationStage.MIN_EDGE_PICKING_1);
			}
			return true;
		case AT_SELF_EDGE_CLEANING_1:
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_SELF_EDGE_CLEANING_2);
			return true;
		case AT_SELF_EDGE_CLEANING_2:
			doAtSelfEdgeCleaning2AndMoveToCompStage(master);		
			return true;
		default:
			return false;
		}
	}

	public static void doAtSelfEdgeCleaning2AndMoveToCompStage(MSTMaster master) {
		IntSumGlobalObject numActiveEdges = (IntSumGlobalObject) master.getGlobalObjectsMap().getGlobalObject(
			MSTOptions.GOBJ_NUM_EDGES_FOR_EDGE_STORAGE_AT_SELF_CUSTOM_COUNTING);
		if (numActiveEdges == null) {
			System.out.println("numActiveEdges is null");
		} else {
			System.out.println("numActiveEdges: " + numActiveEdges.getValue().getValue());
		}
		if ((numActiveEdges == null || numActiveEdges.getValue().getValue() == 0)) {
			master.continueComputation = false;
		} else if (!master.terminateIfNumActiveVerticesIsZero()) {
			master.clearGlobalObjectsAndSetComputationStage(ComputationStage.MIN_EDGE_PICKING_1);
		}
	}
}
