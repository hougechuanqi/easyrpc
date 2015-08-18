package com.immomo.rpc.util;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;

class LogAdapter {
	private Log LOG;
	private Logger LOGGER;

	private LogAdapter(Log LOG) {
		this.LOG = LOG;
	}

	private LogAdapter(Logger LOGGER) {
		this.LOGGER = LOGGER;
	}

	public static LogAdapter create(Log LOG) {
		return new LogAdapter(LOG);
	}

	public static LogAdapter create(Logger LOGGER) {
		return new LogAdapter(LOGGER);
	}

	public void info(String msg) {
		if (LOG != null) {
			LOG.info(msg);
		} else if (LOGGER != null) {
			LOGGER.info(msg);
		}
	}

	public void warn(String msg, Throwable t) {
		if (LOG != null) {
			LOG.warn(msg, t);
		} else if (LOGGER != null) {
			LOGGER.warn(msg, t);
		}
	}

	public void debug(Throwable t) {
		if (LOG != null) {
			LOG.debug(t);
		} else if (LOGGER != null) {
			LOGGER.debug("", t);
		}
	}

	public void error(String msg) {
		if (LOG != null) {
			LOG.error(msg);
		} else if (LOGGER != null) {
			LOGGER.error(msg);
		}
	}
}