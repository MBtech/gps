package gps.node.master.monitoring;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.html.MonitoringMainPage;
import gps.node.master.monitoring.html.StatsPage;
import gps.node.master.monitoring.html.WorkersListPage;

import org.apache.log4j.Logger;

/**
 * An {@link IoHandler} for HTTP.
 */
public class ServerHandler extends BaseServerHandler {

	private static Logger logger = Logger.getLogger(ServerHandler.class);
	private final MonitoringMainPage monitoringMainPage;
	private final WorkersListPage workersListPage;
	private final StatsPage statsPage;

	public ServerHandler(MachineConfig machineConfig, MachineStats machineStats) {
		this.monitoringMainPage = new MonitoringMainPage(machineConfig, machineStats, "");
		this.workersListPage = new WorkersListPage(machineConfig, machineStats, "");
		this.statsPage = new StatsPage(machineStats, machineConfig, "");
	}

	@Override
	protected HttpResponseMessage handleMessageReceived(HttpRequestMessage request,
		String relativeURL) {
		if (MONITORING_HTML.equalsIgnoreCase(relativeURL)) {
			logger.info("Requesting monitoring main page...");
			return monitoringMainPage.generatePage(request);
		} else if (WORKERS_HTML.equals(relativeURL)) {
			logger.info("Requesting workers list page...");
			return workersListPage.generatePage(request);
		} else if (STATS_HTML.equals(relativeURL)) {
			logger.info("Requesting stats list page...");
			return statsPage.generatePage(request);
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