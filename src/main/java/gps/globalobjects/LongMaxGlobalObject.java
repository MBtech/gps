package gps.globalobjects;

import gps.writable.LongWritable;
import gps.writable.MinaWritable;

public class LongMaxGlobalObject extends LongGlobalObject {

	public LongMaxGlobalObject() {
		super();
	}

	public LongMaxGlobalObject(Long value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new LongWritable(Math.max(getValue().getValue(),
			((LongWritable) otherValue).getValue())));
	}
}