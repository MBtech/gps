package gps.globalobjects;

import gps.writable.LongWritable;


public abstract class LongGlobalObject extends GlobalObject<LongWritable>{

	public LongGlobalObject() {
		setValue(new LongWritable((long) -1));
	}

	public LongGlobalObject(Long value) {
		setValue(new LongWritable(value));
	}
}