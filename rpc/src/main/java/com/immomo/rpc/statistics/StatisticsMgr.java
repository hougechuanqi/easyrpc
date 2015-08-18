package com.immomo.rpc.statistics;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.immomo.rpc.ipc.RPCServer;

public class StatisticsMgr {

	public static class StatisticData {
		private long beginTime;
		private long endTime;
		private AtomicInteger recSize = new AtomicInteger(0);
		private AtomicInteger procSize = new AtomicInteger(0);
		private AtomicInteger errorNum = new AtomicInteger(0);
		private int tcpRecNum;
		private int callQueueSize;
		private double cpuLoad;
		private long memFree;
		private int handlerSize;

		public StatisticData(long beginTime) {
			this.beginTime = beginTime;
			this.endTime = beginTime + 1000;
		}

		public long getBeginTime() {
			return beginTime;
		}

		public void setBeginTime(long beginTime) {
			this.beginTime = beginTime;
		}

		public long getEndTime() {
			return endTime;
		}

		public void setEndTime(long endTime) {
			this.endTime = endTime;
		}

		public int getCallQueueSize() {
			return callQueueSize;
		}

		public void setCallQueueSize(int callQueueSize) {
			this.callQueueSize = callQueueSize;
		}

		public AtomicInteger getRecSize() {
			return recSize;
		}

		public void setRecSize(AtomicInteger recSize) {
			this.recSize = recSize;
		}

		public AtomicInteger getProcSize() {
			return procSize;
		}

		public void setProcSize(AtomicInteger procSize) {
			this.procSize = procSize;
		}

		public AtomicInteger getErrorNum() {
			return errorNum;
		}

		public void setErrorNum(AtomicInteger errorNum) {
			this.errorNum = errorNum;
		}

		public int getTcpRecNum() {
			return tcpRecNum;
		}

		public void setTcpRecNum(int tcpRecNum) {
			this.tcpRecNum = tcpRecNum;
		}

		public double getCpuLoad() {
			return cpuLoad;
		}

		public void setCpuLoad(double cpuLoad) {
			this.cpuLoad = cpuLoad;
		}

		public long getMemFree() {
			return memFree;
		}

		public void setMemFree(long memFree) {
			this.memFree = memFree;
		}

		public int getHandlerSize() {
			return handlerSize;
		}

		public void setHandlerSize(int handlerSize) {
			this.handlerSize = handlerSize;
		}
	}
    public static final int MB = 1024*1024;
    public static final boolean isInitialized = true;
	private static final StatisticsMgr mgr = new StatisticsMgr();
	private ConcurrentLinkedQueue<StatisticData> queue = new ConcurrentLinkedQueue<StatisticData>();
	private StatisticData statisticData = new StatisticData(0);
	private RPCServer server;

	private StatisticsMgr() {
	}

	public static StatisticsMgr getInstance() {
		return mgr;
	}
	
	public void init(RPCServer rpcServer){
		this.server = rpcServer;
	}

	// Gen new statistic data and return the head of queue
	public StatisticData getNextStatisticData() {
		getStatisticData();
		return queue.poll();
	}

	public void addRecSize() {
		getStatisticData().getRecSize().incrementAndGet();
	}

	public void addProcSize() {
		getStatisticData().getProcSize().incrementAndGet();
	}

	public void addErrorNum() {
		getStatisticData().getErrorNum().incrementAndGet();
	}

	private StatisticData getStatisticData() {
		long now = System.currentTimeMillis();
		if (now < this.statisticData.getEndTime() && this.statisticData.getBeginTime() != 0) {
			return this.statisticData;
		} else {
			synchronized (mgr) {
				if (now < this.statisticData.getEndTime()) {
					return this.statisticData;
				}
				
				this.statisticData.setCallQueueSize(this.server.getCallQueueSize());
				this.statisticData.setCpuLoad(ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class).getSystemLoadAverage());
				this.statisticData.setHandlerSize(this.server.getHandlerCount());
				this.statisticData.setMemFree(Runtime.getRuntime().freeMemory()/MB);
				this.statisticData.setTcpRecNum(this.server.getTcpConnectNum());
				this.queue.add(this.statisticData);
				
				if(this.statisticData.getBeginTime() == 0){
					this.statisticData.setBeginTime(now);
					this.statisticData.setEndTime(now+1000);
				}

				this.statisticData = new StatisticData(this.statisticData.getEndTime());
				return this.statisticData;
			}
		}
	}
}
