package gps.examples.scc.gobj;

import gps.globalobjects.GlobalObject;
import gps.writable.MinaWritable;

public class SCCStagesMapGObj extends GlobalObject<SCCStagesMapWritable> {

	public SCCStagesMapGObj() {
		setValue(new SCCStagesMapWritable());
	}

	public SCCStagesMapGObj(SCCStagesMapWritable sccStagesMap) {
		setValue(sccStagesMap);
	}
	
	@Override
	public void update(MinaWritable otherWritable) {
	}
}
