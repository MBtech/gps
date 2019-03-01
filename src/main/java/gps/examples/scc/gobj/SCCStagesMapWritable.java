package gps.examples.scc.gobj;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mina.core.buffer.IoBuffer;

import gps.examples.scc.SCCComputationPhase.Phase;
import gps.writable.MinaWritable;

public class SCCStagesMapWritable extends MinaWritable {

	public Map<Integer, Phase> componentKeyStageMap;
	
	public SCCStagesMapWritable() {
		this.componentKeyStageMap = new HashMap<Integer, Phase>();
	}

	public void putStage(Integer componentId, Phase computationStage) {
		componentKeyStageMap.put(componentId, computationStage);
	}

	@Override
	public int numBytes() {
		return 4 + 5 * componentKeyStageMap.size();
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		System.out.println("Writing SCCStagesMapWritable to the IoBuffer.");
		ioBuffer.putInt(componentKeyStageMap.size());
		for (Entry<Integer, Phase> keyValue : componentKeyStageMap.entrySet()) {
			Integer key = keyValue.getKey();
			Phase computationStage = keyValue.getValue();
			ioBuffer.putInt(key);
			ioBuffer.put((byte) computationStage.getId());
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.componentKeyStageMap = new HashMap<Integer, Phase>();
		int size = ioBuffer.getInt();
		for (int i = 0; i < size; ++i) {
			this.componentKeyStageMap.put(ioBuffer.getInt(),
				Phase.getComputationStageFromId(ioBuffer.get()));
		}
	}

	@Override
	public int read(byte[] byteArray, int index) {
		throw new UnsupportedOperationException("reading from the byte[] into java object should never " +
			" be called for this global object writable: " + getClass().getCanonicalName());
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		throw new UnsupportedOperationException("reading from the io buffer into the byte[] should never" +
			" be called for this global object writable: " + getClass().getCanonicalName());
	}
}
