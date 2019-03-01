package gps.node.master.monitoring.debug;

import java.util.HashMap;
import java.util.Map;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.MachineStats.StatName;
import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.BaseServerHandler;
import gps.node.master.monitoring.HttpRequestMessage;
import gps.node.master.monitoring.HttpResponseMessage;
import gps.node.master.monitoring.html.MonitoringMainPage;
import gps.node.master.monitoring.html.StatsPage;
import gps.node.master.monitoring.html.WorkersListPage;

import org.apache.log4j.Logger;

/**
 * An {@link IoHandler} for HTTP.
 */
public class DebugServerHandler extends BaseServerHandler {

	private static Logger logger = Logger.getLogger(DebugServerHandler.class);
	private final DebugMonitoringMainPage debugMonitoringMainPage;
	private final Map<String, MonitoringMainPage> monitoringPageMap;
	private final Map<String, WorkersListPage> workersListPageMap;
	private final Map<String, StatsPage> statsPageMap;

	public DebugServerHandler(Map<String, MachineStats> machineStatsMap) {
		this.debugMonitoringMainPage = new DebugMonitoringMainPage(machineStatsMap.keySet());
		this.monitoringPageMap = new HashMap<String, MonitoringMainPage>();
		this.workersListPageMap = new HashMap<String, WorkersListPage>();
		this.statsPageMap = new HashMap<String, StatsPage>();
		for (String executionName : machineStatsMap.keySet()) {
			MachineConfig machineConfig = new MachineConfig();
			MachineStats machineStats = machineStatsMap.get(executionName);
			for (int i = 0; ;++i) {
				if (machineStats.getStatValue(StatName.NUM_VERTICES.name(), 1, i) != null) {
					logger.info("Adding machineId " + i + " to machineconfig.");
					machineConfig.addMachine(i, "", -1);
				} else {
					break;
				}
			}
			monitoringPageMap.put(executionName, new MonitoringMainPage(machineConfig, machineStats,
				executionName));
			workersListPageMap.put(executionName, new WorkersListPage(machineConfig, machineStats,
				executionName));
			statsPageMap.put(executionName, new StatsPage(machineStats, machineConfig, executionName));
		}
	}

	@Override
	protected HttpResponseMessage handleMessageReceived(HttpRequestMessage request,
		String relativeURL) {
		String executionName = request.getParameter(EXECUTION_NAME_ARG_NAME);
		if (DEBUG_MONITORING_HTML.equalsIgnoreCase(relativeURL)) {
			logger.info("Requesting debug monitoring main page...");
			return debugMonitoringMainPage.generatePage(request);
		} else if (MONITORING_HTML.equalsIgnoreCase(relativeURL)) {
			logger.info("Requesting monitoring main web page list page...");
			return monitoringPageMap.get(executionName).generatePage(request);
		} else if (WORKERS_HTML.equals(relativeURL)) {
			logger.info("Requesting workers list page...");
				return workersListPageMap.get(executionName).generatePage(request);
		} else if (STATS_HTML.equals(relativeURL)) {
			logger.info("Requesting stats list page...");
			return statsPageMap.get(executionName).generatePage(request);
		} else {
			String errorMessage = "Unknown relative URL requested. relativeURL: "
				+ relativeURL;
			logger.error(errorMessage);
			return getErrorResponsePage(errorMessage);
		}
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}