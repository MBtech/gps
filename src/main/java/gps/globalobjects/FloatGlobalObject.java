package gps.globalobjects;

import gps.writable.FloatWritable;

public abstract class FloatGlobalObject extends GlobalObject<FloatWritable>{

	public FloatGlobalObject() {
		setValue(new FloatWritable((float) -1.0));
	}

	public FloatGlobalObject(float value) {
		setValue(new FloatWritable(value));
	}
}
