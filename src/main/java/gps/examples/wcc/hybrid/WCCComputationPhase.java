package gps.examples.wcc.hybrid;

import java.util.HashMap;
import java.util.Map;

public class WCCComputationPhase {

	public static enum Phase {
		ROOT_PICKING(0),
		ROOT_DISCOVERY_AND_SINGLE_LP_1(1),
		SINGLE_LP_REST(2),
		MAX_LP_1(3),
		MAX_LP_REST(4);
		
		private static Map<Integer, Phase> idComputationStateMap =
			new HashMap<Integer, Phase>();
		static {
			for (Phase type : Phase.values()) {
				idComputationStateMap.put(type.id, type);
			}
		}

		private int id;

		private Phase(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

		public static Phase getComputationStageFromId(int id) {
			return idComputationStateMap.get(id);
		}
	}
}
