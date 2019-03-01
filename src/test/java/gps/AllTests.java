package gps;

import gps.node.GPSNodeTests;
import gps.node.MachineConfigTests;
import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests extends TestSuite {

	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(MachineConfigTests.class);
		suite.addTestSuite(GPSNodeTests.class);
		return suite;
	}
}
