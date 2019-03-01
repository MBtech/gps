package gps.examples.extendedsssp;

import java.util.HashMap;
import java.util.Map;

public class ExtendedSSSPComputationStage {

	public static enum ComputationStage {
		SSSP_FIRST_SUPERSTEP(0),
		SSSP_LATER_SUPERSTEP(1),
		AVERAGE_COMPUTATION_STAGE(2);

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
