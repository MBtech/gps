package gps.node.worker;

import gps.node.MachineConfig;
import gps.node.MachineStats;

/**
 * Helper class to maintain the internal state of a GPSNode.
 * 
 * TODO(semih): Consider making this an instantiated class as opposed to a class with static fields.
 * 
 * @author semihsalihoglu
 */
public class GPSWorkerExposedGlobalVariables {

	// TODO(semih): Make the public variables package or protected or refactor accordingly.
	private static volatile int localMachineId;
	private static volatile MachineConfig machineConfig;
	public static volatile int graphSize;
	public static volatile int currentSuperstepNo;
	private static volatile int numWorkers;
	private static int graphPartitionSize;
	public static MachineStats machineStats;

	public static void initVariables(int localMachineId, MachineConfig machineConfig,
		int graphPartitionSize, int graphSize) {
		GPSWorkerExposedGlobalVariables.localMachineId = localMachineId;
		GPSWorkerExposedGlobalVariables.machineConfig = machineConfig;
		GPSWorkerExposedGlobalVariables.graphPartitionSize = graphPartitionSize;
		GPSWorkerExposedGlobalVariables.graphSize = graphSize;
		GPSWorkerExposedGlobalVariables.numWorkers = machineConfig.getWorkerIds().size();
		machineStats = new MachineStats();
	}

	public static MachineStats getMachineStats() {
		return machineStats;
	}

	public static int getLocalMachineId() {
		return localMachineId;
	}

	public static MachineConfig getMachineConfig() {
		return machineConfig;
	}

	public static int getGraphSize() {
		return graphSize;
	}

	public static int getCurrentSuperstepNo() {
		return currentSuperstepNo;
	}

	public static int getNumWorkers() {
		return numWorkers;
	}

	public static int getGraphPartitionSize() {
		return graphPartitionSize;
	}
}
