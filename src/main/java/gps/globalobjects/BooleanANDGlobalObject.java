package gps.globalobjects;

import gps.writable.BooleanWritable;
import gps.writable.MinaWritable;

public class BooleanANDGlobalObject extends BooleanGlobalObject {
	
	public BooleanANDGlobalObject() {
		super();
	}
	
	public BooleanANDGlobalObject(boolean value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new BooleanWritable(getValue().getValue() &&
			((BooleanWritable) otherValue).getValue()));
	}
}
