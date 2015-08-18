package com.immomo.rpc.statistics;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServerStatistics implements Runnable {
	private static final Log LOG = LogFactory.getLog(ServerStatistics.class);
	private static int Num = 0;

	@Override
	public void run() {

		SimpleDateFormat time = new SimpleDateFormat("HH:mm:ss");

		if (Num % 10 == 0) {
			LOG.info(String.format("%-5s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n", "序号", "beginTime", "endTime",
					"CallSize", "RecevSize", "ProcSize", "CPU load", "MEM free", "Handler Size", "Error Num",
					"Recev TCP Num"));
			LOG.info(String.format("%-5s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n", "---", "--------", "-------",
					"--------", "--------", "--------", "--------", "--------", "------------", "---------",
					"-----------"));
		}

		StatisticsMgr.StatisticData data = StatisticsMgr.getInstance().getNextStatisticData();
		if (data != null) {
			LOG.info(String.format("%-5s %10s %10s %10s %10s %10s %10.2f %10s %10s %10s %10s\n", Num % 10 + 1,
					time.format(new Date(data.getBeginTime())), time.format(new Date(data.getEndTime())),
					data.getCallQueueSize(), data.getRecSize().get(), data.getProcSize().get(), data.getCpuLoad(),
					data.getMemFree() + "M", data.getHandlerSize(), data.getErrorNum().get(), data.getTcpRecNum()));
			Num++;
		}

	}
}
