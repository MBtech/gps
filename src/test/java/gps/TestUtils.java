package gps;

import gps.node.MachineConfig;

public class TestUtils {
	public static int localMachineId = 0;
	public static String localMachineHost = "localhost";
	public static int localMachinePort = 216;
	public static int otherMachineId1 = 1;
	public static String otherMachineHost1 = "otherMachine1";
	public static int otherMachinePort1 = 32512;
	public static int otherMachineId2 = 2;
	public static String otherMachineHost2 = "otherMachine2";
	public static int otherMachinePort2 = 2563;

	public static MachineConfig machineConfig;

	public static void init() {
		machineConfig = new MachineConfig();
		machineConfig.addMachine(localMachineId, localMachineHost, localMachinePort)
			.addMachine(otherMachineId1, otherMachineHost1, otherMachinePort1)
			.addMachine(otherMachineId2, otherMachineHost2, otherMachinePort2);
	}
}
