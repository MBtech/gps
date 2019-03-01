package gps.globalobjects;

import gps.writable.MinaWritable;

import org.apache.log4j.Logger;

// TODO(semih): Remove global objects and just use writables because this update method is
// the same as the combine method in MinaWritable.
public abstract class GlobalObject<V extends MinaWritable> {

	private static Logger logger = Logger.getLogger(GlobalObject.class);

	private V value;
	
	public abstract void update(MinaWritable minaWritable);
	
	public V getValue() {
		return value;
	}
	
	public void setValue(V value) {
		this.value = value;
	}
}