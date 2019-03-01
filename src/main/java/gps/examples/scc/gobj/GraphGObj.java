package gps.examples.scc.gobj;

import java.util.Map;
import java.util.Map.Entry;

import gps.globalobjects.GlobalObject;
import gps.writable.MinaWritable;

public class GraphGObj extends GlobalObject<GraphWritable>{

	public GraphGObj() {
		setValue(new GraphWritable());
	}

	public GraphGObj(Map<Integer, NodeWritable> graph) {
		setValue(new GraphWritable(graph));
	}

	@Override
	public void update(MinaWritable minaWritable) {
		GraphWritable otherGraphWritable = (GraphWritable) minaWritable;
		GraphWritable thisGraphWritable = getValue();
		for (Entry<Integer, NodeWritable> node : otherGraphWritable.graph.entrySet()) {
			thisGraphWritable.graph.put(node.getKey(), node.getValue());
		}
	}
}
