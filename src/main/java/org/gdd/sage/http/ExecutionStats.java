package org.gdd.sage.http;

import org.apache.jena.ext.com.google.common.math.Stats;

import java.util.ArrayList;
import java.util.List;

/**
 * An utility class used to record statistic during query execution
 * @author Thomas Minier
 */
public class ExecutionStats {
    private double transferSize = 0;
    private double executionTime;
    private int nbCalls;
    private List<Double> httpTimes;
    private List<Double> resumeTimes;
    private List<Double> suspendTimes;
    private List<Double> decodingResponseTimes;
    private List<String> logs;
    private boolean printLogs = false;

    public ExecutionStats() {
        executionTime = -1;
        nbCalls = 0;
        httpTimes = new ArrayList<>();
        resumeTimes = new ArrayList<>();
        suspendTimes = new ArrayList<>();
        decodingResponseTimes = new ArrayList<>();
        logs = new ArrayList<>();
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public List<String> getlogs() {
        return this.logs;
    }

    public void setPrintLogs(boolean print) {
        this.printLogs = print;
    }

    public List<Double> getDecodingResponseTimes() { return this.decodingResponseTimes; }

    public int getNbCalls() {
        return nbCalls;
    }

    public Double getMeanHttpTimes() {
        if (httpTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(httpTimes);
    }

    public Double getMeanResumeTime() {
        if (resumeTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(resumeTimes);
    }

    public Double getMeanSuspendTime() {
        if (suspendTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(suspendTimes);
    }

    public Double getMeanDecodingResponseTime() {
        if (decodingResponseTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(decodingResponseTimes);
    }

    public void startTimer() {
        executionTime = System.nanoTime();
    }

    public void stopTimer() {
        double endTime = System.nanoTime();
        executionTime = (endTime - executionTime) / 1e9;
    }

    public void reportHttpQuery(double execTime) {
        nbCalls++;
        httpTimes.add(execTime);
    }

    public void reportOverhead(double resumeTime, double suspendTime) {
        resumeTimes.add(resumeTime);
        suspendTimes.add(suspendTime);
    }

    public double getTransferSize() {
        return transferSize;
    }

    public void reportTransferSize(int length) {
        transferSize += length;
    }

    public void reportDecodingResponseTime(double decodingTimeEnd) {
        this.decodingResponseTimes.add(decodingTimeEnd);
    }

    public void reportLog(String s) {
        if (this.printLogs) System.err.println(s);
        logs.add(s);
    }
}
