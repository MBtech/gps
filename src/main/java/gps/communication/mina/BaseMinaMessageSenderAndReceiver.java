package gps.communication.mina;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.MachineStats.StatName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;

public abstract class BaseMinaMessageSenderAndReceiver {

	protected ConcurrentHashMap<Integer, IoSession> sessionsMap
		= new ConcurrentHashMap<Integer, IoSession>();
	protected int numEstablishedConnections = 0;

	protected final MachineConfig machineConfig;
	protected final int localMachineId;
	protected ServerConnectionsEstablisher serverConnectionsEstablisher;
	protected ClientConnectionsEstablisher clientConnectionsEstablisher;
	protected final MachineStats machineStats;
	
	protected BaseMinaMessageSenderAndReceiver(MachineConfig machineConfig, int localMachineId,
		MachineStats machineStats) {
		this.machineConfig = machineConfig;
		this.localMachineId = localMachineId;
		this.machineStats = machineStats;
	}

	public void finishEstablishingAllConnections() {
		while (numEstablishedConnections != (machineConfig.getAllMachineIds().size() - 1)) {
			getLogger().info("sleeping for 500 millis. numEstablishedConnection: "
				+ numEstablishedConnections);
			ArrayList<Integer> sortedMachineIds = new ArrayList<Integer>(sessionsMap.keySet());
			Collections.sort(sortedMachineIds);
			getLogger().info("Dumping established connections");
			for (int machineId : sortedMachineIds) {
				getLogger().info(machineId);
			}
			getLogger().info("End of dumping established connections");
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void startEstablishingAllConnections() {
		serverConnectionsEstablisher.start();
		clientConnectionsEstablisher.start();
	}

	public synchronized void putSession(int machineId, IoSession session) {
		getLogger().info("putSession: putting machineId: " + machineId + " machineIdAttribute: "
			+ (Integer) session.getAttribute("machineId"));
		sessionsMap.put(machineId, session);
		machineStats.updateGlobalStat(StatName.CONNECTION_ESTABLISHMENT_TIMESTAMP, machineId,
			(double) System.currentTimeMillis());
		numEstablishedConnections++;
	}

	public void closeServerSocket() {
		serverConnectionsEstablisher.cleanUp();
	}

	protected abstract Logger getLogger();
}
