package gps.globalobjects;

import gps.writable.FloatWritable;
import gps.writable.MinaWritable;

public class FloatProductGlobalObject extends FloatGlobalObject {

	public FloatProductGlobalObject() {
		super();
	}

	public FloatProductGlobalObject(float value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new FloatWritable(getValue().getValue() *
			((FloatWritable) otherValue).getValue()));
	}
}
