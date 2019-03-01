package gps.communication;

import gps.messages.OutgoingBufferedMessage;

public interface BaseMessageSenderAndReceiver {

	public void startEstablishingAllConnections();

	public void finishEstablishingAllConnections();

	public void closeServerSocket();

	/**
	 * Sends the given message to the machine with the specified machineId
	 * 
	 * @param outgoingBufferedMessage message to send to
	 * @param toMachineId id of the receiving message
	 */
	public void sendBufferedMessage(OutgoingBufferedMessage outgoingBufferedMessage,
		int toMachineId);
}