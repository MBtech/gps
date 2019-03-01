package gps.writable;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;

public class StreamingRootPickerValue {
	public int[] kValues;
	public int numValuesInserted;
	private Random random;
	
	public StreamingRootPickerValue(int k) {
		this.kValues = new int[k];
		this.numValuesInserted = 0;
		this.random = new Random();
	}
	
	public StreamingRootPickerValue(int intValue, int k) {
		this(k);
		this.kValues[0] = intValue;
		this.numValuesInserted++;
	}

	@VisibleForTesting
	public StreamingRootPickerValue(int[] kValues, int numValuesInserted) {
		this.kValues = kValues;
		this.numValuesInserted = numValuesInserted;
		this.random = new Random();
	}

	public void insertNewIntValues(StreamingRootPickerValue value2) {
		if (numValuesInserted < kValues.length) {
			for (int i = 0; i < numValuesInserted; ++i) {
				value2.insertNewIntValue(kValues[i]);
			}
			kValues = value2.kValues;
			numValuesInserted = value2.numValuesInserted;
			return;
		} else if (value2.numValuesInserted < kValues.length) {
			for (int i = 0; i < value2.numValuesInserted; ++i) {
				insertNewIntValue(value2.kValues[i]);
			}
			return;
		} else {
			List<Integer> listOfPositions = new LinkedList<Integer>();
			for (int i = 0; i < kValues.length; ++i) {
				listOfPositions.add(i);
			}
			double pickProbability = ((double) value2.numValuesInserted /
				(double) (numValuesInserted + value2.numValuesInserted));
			for (int valueToInsert : value2.kValues) {
				if (random.nextDouble() < pickProbability) {
					int positionToReplace = listOfPositions.remove(
						random.nextInt(listOfPositions.size()));
					kValues[positionToReplace] = valueToInsert;
				}
			}
		}
		numValuesInserted += value2.numValuesInserted;
	}
	
	public void insertNewIntValue(int intValue) {
		if (numValuesInserted < kValues.length) {
			kValues[numValuesInserted] = intValue;
			numValuesInserted++;
		} else {
			if (random.nextDouble() < ((double) kValues.length / (numValuesInserted + 1))) {
				kValues[random.nextInt(kValues.length)] = intValue;
			}
			numValuesInserted++;
		}
	}
	
	public byte getValueIndex(int intValue) {
		for (int i = 0; i < Math.min(numValuesInserted, kValues.length); ++i) {
			if (kValues[i] == intValue) {
				return (byte) i;
			}
		}
		return (byte) -1;
	}

	@Override
	public boolean equals(Object o) {
		if(o == this) return true;
		if(o == null || !(o instanceof StreamingRootPickerValue)) return false;
		StreamingRootPickerValue other = StreamingRootPickerValue.class.cast(o);
		return this.numValuesInserted == other.numValuesInserted && Arrays.equals(this.kValues,
			other.kValues);
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("numValuesInserted: " + numValuesInserted);
		for (int i = 0; i < Math.min(numValuesInserted, kValues.length); ++i) {
			stringBuilder.append(" " + kValues[i]);
		}
		stringBuilder.append("\n");
		return stringBuilder.toString();
	}

	@VisibleForTesting
	public void setRandomForTesting(Random random) {
		this.random = random;
	}
}