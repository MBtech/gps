package gps.examples.scc;

import java.util.Arrays;
import java.util.Random;

import org.easymock.EasyMock;

import gps.EasyMockTest;
import gps.writable.StreamingRootPickerValue;

public class StreamingRootPickerValueTest extends EasyMockTest {

	public void testStoringOneValue() {
		int k = 2;
		StreamingRootPickerValue rootPickerValue = new StreamingRootPickerValue(5, k);
		assertEquals(1, rootPickerValue.numValuesInserted);
		assertTrue(Arrays.equals(new int[] {5, 0}, rootPickerValue.kValues));
	}
	
	public void testAddingMultipleValuesLessThanK() {
		int k = 3;
		StreamingRootPickerValue rootPickerValue = new StreamingRootPickerValue(145, k);		
		rootPickerValue.insertNewIntValue(634);
		assertEquals(2, rootPickerValue.numValuesInserted);
		assertTrue(Arrays.equals(new int[] {145, 634, 0}, rootPickerValue.kValues));
	}

	public void testAddingMultipleValuesMoreThanK() {
		int k = 2;
		Random mockRandom = mocksControl.createMock(Random.class);
		EasyMock.expect(mockRandom.nextDouble()).andReturn(0.51); // Next update should go in
		EasyMock.expect(mockRandom.nextInt(2)).andReturn(0); // And replace the first inserted element
		EasyMock.expect(mockRandom.nextDouble()).andReturn(0.51); // Next update should not go in
		mocksControl.replay();
		StreamingRootPickerValue rootPicker = new StreamingRootPickerValue(1, k);
		rootPicker.setRandomForTesting(mockRandom);
		rootPicker.insertNewIntValue(2);
		rootPicker.insertNewIntValue(3);
		rootPicker.insertNewIntValue(4);
		assertEquals(4, rootPicker.numValuesInserted);
		assertTrue(Arrays.equals(new int[] {3, 2}, rootPicker.kValues));
		mocksControl.verify();
	}

	public void testInsertNewValuesFirstMapHasLessThanKValues() {
		int k = 5;
		Random mockRandom = mocksControl.createMock(Random.class);
		EasyMock.expect(mockRandom.nextDouble()).andReturn(0.81); // Next update should not go in
		EasyMock.expect(mockRandom.nextDouble()).andReturn(0.01); // Next update should go in
		EasyMock.expect(mockRandom.nextInt(5)).andReturn(3); // And replace the fourth inserted element
		mocksControl.replay();
		StreamingRootPickerValue value1 = new StreamingRootPickerValue(new int[] {0, 1, 0, 0, 0}, 2);
		StreamingRootPickerValue value2 = new StreamingRootPickerValue(new int[] {10, 11, 12, 13, 14}, 10);
		value2.setRandomForTesting(mockRandom);
		value1.insertNewIntValues(value2);
		int[] expectedKValues = new int[] {10, 11, 12, 1, 14};
		StreamingRootPickerValue expectedValue = new StreamingRootPickerValue(expectedKValues, 12);
		assertEquals(expectedValue, value1);
		mocksControl.verify();
	}

	public void testInsertNewValuesSecondMapHasLessThanKValues() {
		setUp(true);
		int k = 5;
		Random mockRandom = mocksControl.createMock(Random.class);
		EasyMock.expect(mockRandom.nextDouble()).andReturn(((double) 5 / (double) 16) + 0.01); // Next update should not go in
		EasyMock.expect(mockRandom.nextDouble()).andReturn(((double) 5 / (double) 17) - 0.01); // Next update should go in
		EasyMock.expect(mockRandom.nextInt(5)).andReturn(0); // And replace the first inserted element
		EasyMock.expect(mockRandom.nextDouble()).andReturn(((double) 5 / (double) 18) - 0.02); // Next update should go in
		EasyMock.expect(mockRandom.nextInt(5)).andReturn(2); // And replace the third inserted element
		mocksControl.replay();
		StreamingRootPickerValue value1 = new StreamingRootPickerValue(new int[] {0, 1, 2, 3, 4}, 15);
		value1.setRandomForTesting(mockRandom);
		StreamingRootPickerValue value2 = new StreamingRootPickerValue(new int[] {10, 11, 12, 0, 0}, 3);
		value1.insertNewIntValues(value2);
		int[] expectedKValues = new int[] {11, 1, 12, 3, 4};
		StreamingRootPickerValue expectedValue = new StreamingRootPickerValue(expectedKValues, 18);
		assertEquals(expectedValue, value1);
		mocksControl.verify();
	}

	public void testInsertNewValuesBothMapsHaveMoreThanKValues() {
		setUp(true);
		int k = 3;
		Random mockRandom = mocksControl.createMock(Random.class);
		EasyMock.expect(mockRandom.nextDouble()).andReturn(((double) 10 / (double) 25) + 0.01); // Next update should not go in
		EasyMock.expect(mockRandom.nextDouble()).andReturn(((double) 10 / (double) 25) - 0.01); // Next update should go in
		EasyMock.expect(mockRandom.nextInt(3)).andReturn(1); // And replace the second inserted element
		EasyMock.expect(mockRandom.nextDouble()).andReturn(((double) 10 / (double) 25) - 0.02); // Next update should go in
		// And replace the third inserted element (once the second is removed, then position 1 becomes the 3rd element)
		EasyMock.expect(mockRandom.nextInt(2)).andReturn(1);
		mocksControl.replay();
		StreamingRootPickerValue value1 = new StreamingRootPickerValue(new int[] {0, 1, 2}, 15);
		value1.setRandomForTesting(mockRandom);
		StreamingRootPickerValue value2 = new StreamingRootPickerValue(new int[] {10, 11, 12}, 10);
		value1.insertNewIntValues(value2);
		int[] expectedKValues = new int[] {0, 11, 12};
		StreamingRootPickerValue expectedValue = new StreamingRootPickerValue(expectedKValues, 25);
		assertEquals(expectedValue, value1);
		mocksControl.verify();
	}
}
