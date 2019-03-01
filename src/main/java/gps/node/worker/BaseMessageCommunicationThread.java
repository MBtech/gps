package gps.node.worker;

import gps.node.Utils;

import org.apache.log4j.Logger;

public abstract class BaseMessageCommunicationThread extends Thread {

	protected final Utils utils;
	protected final int localMachineId;
	protected final int connectingMachineId;
	protected final Logger logger;

	public BaseMessageCommunicationThread(String threadName, int localMachineId, Utils utils,
		Logger logger, int connectingMachineId) {
		super(threadName);
		this.localMachineId = localMachineId;
		this.utils = utils;
		this.logger = logger;
		this.connectingMachineId = connectingMachineId;
	}

	protected void printException(Exception e) {
		logger.error(this.getName() + "\terrorMessage:" + e.getMessage());
		e.printStackTrace();
	}
}
