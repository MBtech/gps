package gps.globalobjects;

import gps.writable.DoubleWritable;

public abstract class DoubleGlobalObject extends GlobalObject<DoubleWritable>{

	public DoubleGlobalObject() {
		setValue(new DoubleWritable(-1.0));
	}

	public DoubleGlobalObject(double value) {
		setValue(new DoubleWritable(value));
	}
}
