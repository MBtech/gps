package gps.node.master.monitoring;

import org.apache.log4j.Logger;

public class Constants {
	public static final String DEBUG_MONITORING_HTML = "debugmonitoring.html";
	public static final String MONITORING_HTML = "monitoring.html";
	public static final String STATS_HTML = "stats.html";
	public static final String WORKERS_HTML = "workers.html";
	public static String SUPERSTEP_NO_ARG_NAME = "s_no";
	public static String WORKER_ID_ARG_NAME = "w_id";
	public static String STAT_NAME_ARG_NAME = "stat-name";
	public static String LIST_ARG_NAME = "list";
	public static String EXECUTION_NAME_ARG_NAME = "e_id";
	public static String VERBOSE_ARG_NAME = "verbose";

	public static String REFRESH_ARG_NAME = "refresh";
	public static int DEFAULT_REFRESH_TIME = 10000;
	public static int LONG_REFRESH_TIME = 60000;
	
	public static Integer getIntegerArgumentOrNull(HttpRequestMessage request, String argName,
		Logger logger) {
		String argValue = request.getParameter(argName);
		if (argValue == null) {
			return null;
		}
		try {
			return Integer.parseInt(argValue);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			logger.error("Exception in parsing integer argument value. argName: " + argName
				+ " argValue: " + argValue + "\nError Message: " + e.getMessage());
			return null;
		}
	}
}