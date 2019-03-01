package gps.globalobjects;

import gps.globalobjects.GlobalObject;
import gps.writable.MinaWritable;
import gps.writable.NullValueGraphWritable;

public class NullValueGraphGObj extends GlobalObject<NullValueGraphWritable>{

	public NullValueGraphGObj() {
		setValue(new NullValueGraphWritable());
	}

	public NullValueGraphGObj(NullValueGraphWritable value) {
		setValue(value);
	}
	
	@Override
	public void update(MinaWritable otherWritable) {
		NullValueGraphWritable otherNodes = (NullValueGraphWritable) otherWritable;
		getValue().nodes.addAll(otherNodes.nodes);
	}
}