package gps.globalobjects;

import gps.writable.IntWritable;

public abstract class IntGlobalObject extends GlobalObject<IntWritable>{

	public IntGlobalObject() {
		setValue(new IntWritable(-1));
	}

	public IntGlobalObject(int value) {
		setValue(new IntWritable(value));
	}
}