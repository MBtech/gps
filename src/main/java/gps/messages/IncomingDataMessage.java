package gps.messages;

public class IncomingDataMessage {

	public double messageValue;
	public int fromMachineId;
	public int superstepNo;

	public IncomingDataMessage(double messageValue, int fromMachineId, int superstepNo) {
		this.messageValue = messageValue;
		this.fromMachineId = fromMachineId;
		this.superstepNo = superstepNo;
	}
}
