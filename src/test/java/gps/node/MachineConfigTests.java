package gps.node;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

public class MachineConfigTests extends TestCase {
	// TODO(semih): When there are enough constants extract into a common utility file
	public static String BASE_TEST_DATA_DIR = "/Users/semihsalihoglu/projects/GPS/test_files/";

	private MachineConfig machineConfig;

	@Override
	protected void setUp() {
		machineConfig = new MachineConfig();
	}

	// TODO(semih): Write two simple tests
	public void testAddMachine() {
		int machineId1 = 4;
		String hostName1 = "dummyHost1";
		int port1 = 12525;
		int machineId2 = 111;
		String hostName2 = "dummyHost2";
		int port2 = 235125;
		machineConfig.addMachine(machineId1, hostName1, port1).addMachine(machineId2, hostName2,
			port2);
		List<Integer> allmachineIds = machineConfig.getWorkerIds();
		assertEquals(2, allmachineIds.size());
		assertTrue(allmachineIds.contains(machineId1));
		assertTrue(allmachineIds.contains(machineId2));
		assertEquals(machineId2, machineConfig.getMaxmachineId());
		assertMachineExists(machineConfig, machineId1, hostName1, port1);
		assertMachineExists(machineConfig, machineId2, hostName2, port2);
	}

	public void testLoad() throws IOException {
//		machineConfig.load(BASE_TEST_DATA_DIR + "GPS/node/machine_config_test.cfg");
		assertEquals(5, machineConfig.getWorkerIds().size());
		assertEquals(124, machineConfig.getMaxmachineId());
		assertMachineExists(machineConfig, 34, "localhost", 4652);
		assertMachineExists(machineConfig, 1, "zarya.stanford.edu", 2352);
		assertMachineExists(machineConfig, 124, "dummy-machine1.blah1.blah1", 1155);
		assertMachineExists(machineConfig, 2, "dummy-machine2.blah2.blah2", 12);
		assertMachineExists(machineConfig, 3, "dummy-machine3.blah3.blah3", 5421);
	}

	public void assertMachineExists(MachineConfig machineConfig, int machineId, String hostName,
		int port) {
		Pair<String, Integer> hostPortPair1 = machineConfig.getHostPortPair(machineId);
		assertEquals(hostName, hostPortPair1.fst);
		assertEquals(port, (int) hostPortPair1.snd);
	}
}
