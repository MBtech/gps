package gps.messages;

import org.apache.mina.core.buffer.IoBuffer;

public class IncomingBufferedMessage extends OutgoingBufferedMessage {
	private int fromMachineId;

	public IncomingBufferedMessage(MessageTypes type, int superstepNo, IoBuffer ioBuffer) {
		super(type, superstepNo, ioBuffer);
	}

	public void setFromMachineId(int fromMachineId) {
		this.fromMachineId = fromMachineId;
	}

	public int getFromMachineId() {
		return fromMachineId;
	}
}
