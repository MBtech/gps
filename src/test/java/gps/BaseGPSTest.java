package gps;


import gps.node.Utils;

import java.util.List;

public class BaseGPSTest extends EasyMockTest {

	protected Utils mockUtils;

	@Override
	protected void setUpRest() {
		mockUtils = mocksControl.createMock(Utils.class);
	}

	private static boolean isSameIntegerListWithoutOrder(
		List<Integer> expectedNeighborsOfShuffledNode, List<Integer> actualNeighborsOfShuffledNode) {
		if (expectedNeighborsOfShuffledNode == null) {
			return (actualNeighborsOfShuffledNode == null);
		} else if (actualNeighborsOfShuffledNode == null) {
			return false;
		} else {
			if (expectedNeighborsOfShuffledNode.size() != actualNeighborsOfShuffledNode.size()) {
				return false;
			}
			for (int expectedInt : expectedNeighborsOfShuffledNode) {
				boolean found = false;
				int foundIndex = -1;
				for (int i = 0; i < actualNeighborsOfShuffledNode.size(); ++i) {
					int actualInt = actualNeighborsOfShuffledNode.get(i);
					if (expectedInt == actualInt) {
						found = true;
						foundIndex = i;
						break;
					}
				}
				if (!found) {
					return false;
				}
				expectedNeighborsOfShuffledNode.remove(foundIndex);
			}
		}

		return true;
	}
}
