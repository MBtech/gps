package gps.examples.sssp.flps;

import java.util.HashMap;
import java.util.Map;

public class SSSPFLPSPhase {
	public static enum Phase {
		REGULAR_SSSP(0),
		GRAPH_FORMATION(1),
		SERIAL_RESULT_FINDING(2);
		
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