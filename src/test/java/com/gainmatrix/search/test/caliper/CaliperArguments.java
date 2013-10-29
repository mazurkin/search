package com.gainmatrix.search.test.caliper;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CaliperArguments {

    private static final String SEPARATOR = ",";

    private Class<?> clazz;

    private Collection<String> javaVms;

    private Map<String,List<String>> javaArguments;

    private Map<String,List<String>> parameters;

    private Collection<String> options;

    private CaliperTimeUnit timeUnit = CaliperTimeUnit.MILLISECOND;

    private int warmUpMs = 1000;

    private int runMs = 1000;

    private int trials = 1;

    private boolean debug;

    private boolean captureVmLog;

    public CaliperArguments() {
        this.javaVms = new ArrayList<String>();
        this.javaArguments = new HashMap<String, List<String>>();
        this.parameters = new HashMap<String, List<String>>();
        this.options = new ArrayList<String>();

        this.javaVms.add(defaultJavaVm());
    }

    public void addJavaArgument(String name, String value) {
        List<String> values = javaArguments.get(name);

        if (values == null) {
            values = new ArrayList<String>();
            javaArguments.put(name, values);
        }

        values.add(value);
    }

    public void addJavaArguments(String name, Collection<String> value) {
        List<String> values = javaArguments.get(name);

        if (values == null) {
            values = new ArrayList<String>();
            javaArguments.put(name, values);
        }

        values.addAll(value);
    }

    public List<String> getJavaArguments(String name) {
        return javaArguments.get(name);
    }

    public void addParameter(String name, String value) {
        List<String> values = parameters.get(name);

        if (values == null) {
            values = new ArrayList<String>();
            parameters.put(name, values);
        }

        values.add(value);
    }

    public void addParameters(String name, Collection<String> value) {
        List<String> values = parameters.get(name);

        if (values == null) {
            values = new ArrayList<String>();
            parameters.put(name, values);
        }

        values.addAll(value);
    }

    public List<String> getParameters(String name) {
        return parameters.get(name);
    }

    public String defaultJavaVm() {
        StringBuilder java = new StringBuilder();

        java.append(SystemUtils.JAVA_HOME);
        java.append(SystemUtils.FILE_SEPARATOR);
        java.append("bin");
        java.append(SystemUtils.FILE_SEPARATOR);
        java.append(SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");

        return java.toString();
    }

    public Collection<String> getJavaVms() {
        return javaVms;
    }

    public void addJavaVm(String javaVm) {
        this.javaVms.add(javaVm);
    }

    public void addJavaVms(Collection<String> javaVms) {
        this.javaVms.addAll(javaVms);
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public void setClazz(Class<?> clazz) {
        this.clazz = clazz;
    }

    public int getWarmUpMs() {
        return warmUpMs;
    }

    public void setWarmUpMs(int warmUpMs) {
        this.warmUpMs = warmUpMs;
    }

    public int getRunMs() {
        return runMs;
    }

    public void setRunMs(int runMs) {
        this.runMs = runMs;
    }

    public int getTrials() {
        return trials;
    }

    public void setTrials(int trials) {
        this.trials = trials;
    }

    public CaliperTimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(CaliperTimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isCaptureVmLog() {
        return captureVmLog;
    }

    public void setCaptureVmLog(boolean captureVmLog) {
        this.captureVmLog = captureVmLog;
    }

    public Collection<String> getOptions() {
        return options;
    }

    public void addOption(String option) {
        this.options.add(option);
    }

    public void addOptions(Collection<String> options) {
        this.options.addAll(options);
    }

    public String[] toArgumentArray() {
        Collection<String> arguments = new ArrayList<String>();

        arguments.add("--warmupMillis");
        arguments.add(Integer.toString(warmUpMs));

        arguments.add("--runMillis");
        arguments.add(Integer.toString(runMs));

        arguments.add("--delimiter");
        arguments.add(SEPARATOR);

        arguments.add("--timeUnit");
        arguments.add(timeUnit.getTag());

        if (debug) {
            arguments.add("--debug");
        }

        if (captureVmLog) {
            arguments.add("--captureVmLog");
        }

        arguments.add("--trials");
        arguments.add(Integer.toString(trials));

        arguments.add("--vm");
        arguments.add(StringUtils.join(javaVms, SEPARATOR));

        for (Map.Entry<String,List<String>> entry : javaArguments.entrySet()) {
            arguments.add("-J" + entry.getKey() + "=" + StringUtils.join(entry.getValue(), SEPARATOR));
        }

        for (Map.Entry<String,List<String>> entry : parameters.entrySet()) {
            arguments.add("-D" + entry.getKey() + "=" + StringUtils.join(entry.getValue(), SEPARATOR));
        }

        for (String option : options) {
            arguments.add(option);
        }

        arguments.add(clazz.getCanonicalName());

        return arguments.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

}
