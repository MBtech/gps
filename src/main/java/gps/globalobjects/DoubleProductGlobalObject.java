package gps.globalobjects;

import gps.writable.DoubleWritable;
import gps.writable.MinaWritable;

public class DoubleProductGlobalObject extends DoubleGlobalObject {

	public DoubleProductGlobalObject() {
		super();
	}

	public DoubleProductGlobalObject(double value) {
		super(value);
	}

	@Override
	public void update(MinaWritable otherValue) {
		setValue(new DoubleWritable(getValue().getValue() *
			((DoubleWritable) otherValue).getValue()));
	}
}
