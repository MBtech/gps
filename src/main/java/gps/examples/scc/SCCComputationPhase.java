package gps.examples.scc;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains the enum of different phases used in differen SCC algorithms.
 *
 * @author semihsalihoglu
 */
public class SCCComputationPhase {
	public static enum Phase {
		ROOT_PICKING(0),
		ROOT_DISCOVERY_AND_FW_1(1),
		FW_1(2),
		FW_REST(3),
		BW_1(4),
		BW_REST(5),
		EDGE_CLEANING_1(7),
		EDGE_CLEANING_2(8),
		EDGE_CLEANING_3_AND_TRIMMING(9),
		BW_TRAVERSAL_GRAPH_FORMATION(10),
		FINISHING_GRAPH_FORMATION(11),
		BW_SERIAL_TRAVERSAL_RESULT_FINDING(12),
		FINISHING_SERIAL_COMPUTATION_RESULT_FINDING(13);

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
