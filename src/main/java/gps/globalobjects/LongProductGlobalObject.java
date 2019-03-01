package gps.globalobjects;

import gps.writable.LongWritable;
import gps.writable.MinaWritable;

public class LongProductGlobalObject extends LongGlobalObject {

	public LongProductGlobalObject() {
		super();
	}

	public LongProductGlobalObject(Long value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new LongWritable(getValue().getValue() * ((LongWritable) otherValue).getValue()));
	}
}