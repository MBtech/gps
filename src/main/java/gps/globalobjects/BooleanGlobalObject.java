package gps.globalobjects;

import gps.writable.BooleanWritable;

public abstract class BooleanGlobalObject extends GlobalObject<BooleanWritable>{
	
	public BooleanGlobalObject() {
		setValue(new BooleanWritable(false));
	}
	
	public BooleanGlobalObject(boolean value) {
		setValue(new BooleanWritable(value));
	}
}
