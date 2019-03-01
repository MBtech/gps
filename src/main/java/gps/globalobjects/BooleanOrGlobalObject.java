package gps.globalobjects;

import gps.writable.BooleanWritable;
import gps.writable.MinaWritable;

public class BooleanOrGlobalObject extends BooleanGlobalObject {
	
	public BooleanOrGlobalObject() {
		super();
	}
	
	public BooleanOrGlobalObject(boolean value) {
		super(value);
	}
	@Override
	public void update(MinaWritable otherValue) {
		setValue(new BooleanWritable(getValue().getValue() ||
			((BooleanWritable) otherValue).getValue()));
	}
}