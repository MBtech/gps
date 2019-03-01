package gps.globalobjects;

import gps.writable.LongWritable;
import gps.writable.MinaWritable;

public class LongSumGlobalObject extends LongGlobalObject {

	public LongSumGlobalObject() {
		super();
	}

	public LongSumGlobalObject(Long value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new LongWritable(getValue().getValue() + ((LongWritable) otherValue).getValue()));
	}
}