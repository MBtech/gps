package gps.examples.mst;

import java.util.HashMap;
import java.util.Map;

public class MSTComputationStage {
	public static enum ComputationStage {
		IMMEDIATE_MIN_EDGE_PICKING(0),
		MIN_EDGE_PICKING_1(1), // When storing edges at self
		MIN_EDGE_PICKING_2(2), // When storing edges at self
		POINTER_JUMPING_QUESTION_1(3),
		POINTER_JUMPING_QUESTION(4),
		POINTER_JUMPING_ANSWER(5),
		AT_ROOT_EDGE_CLEANING_1(6),
		AT_ROOT_EDGE_CLEANING_2(7),
		AT_ROOT_EDGE_CLEANING_3(8),
		AT_SELF_EDGE_CLEANING_1(9),
		AT_SELF_EDGE_CLEANING_2(10),
		NOTIFY_NEW_ROOT_THAT_SELF_IS_A_SUBVERTEX(11),
		NOTIFY_SUBVERTICES_OF_NEW_ROOT_1(12),
		NOTIFY_SUBVERTICES_OF_NEW_ROOT_2(13),
		ECOD_MIN_EDGE_PICKING_QUESTION(14),
		ECOD_MIN_EDGE_PICKING_ANSWER_SENDING(15),
		ECOD_MIN_EDGE_PICKING_ANSWER_RECEIVING(16),
		ECOD_EDGE_CLEANING_1(17),
		ECOD_EDGE_CLEANING_2(18),
		ECOD_EDGE_CLEANING_3(19);
		private static Map<Integer, ComputationStage> idComputationStateMap =
			new HashMap<Integer, ComputationStage>();
		static {
			for (ComputationStage type : ComputationStage.values()) {
				idComputationStateMap.put(type.id, type);
			}
		}

		private int id;

		private ComputationStage(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

		public static ComputationStage getComputationStageFromId(int id) {
			return idComputationStateMap.get(id);
		}
	}
}