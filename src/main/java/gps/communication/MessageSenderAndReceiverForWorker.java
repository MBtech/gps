package gps.communication;

import gps.node.StatusType;
import gps.node.worker.AbstractGPSWorker;

/**
 * This is the helper class through which {@link AbstractGPSWorker} communicates with other
 * machines. It is responsible for implementing the low-levels of how messages are sent and
 * received. It makes experimenting with different message transportation methods (TCP, HTTP,
 * asynchronous or in batches, through HDFS, etc.) easier.
 * 
 * @author semihsalihoglu
 * 
 */
public interface MessageSenderAndReceiverForWorker extends BaseMessageSenderAndReceiver {

	public abstract void sendStatusUpdateToMaster(int superstepNo, StatusType statusType);

	public abstract void sendFinalDataSentControlMessagesToAllWorkers(int superstepNo);

	public abstract void sendFinalInitialVertexPartitioningControlMessagesToAllWorkers();

	public int getNumOutgoingBuffersInQueue();
}
