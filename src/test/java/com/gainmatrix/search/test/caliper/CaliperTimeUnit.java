package com.gainmatrix.search.test.caliper;

public enum CaliperTimeUnit {

    NANOSECOND("ns"),

    MICROSECOND("us"),

    MILLISECOND("ms"),

    SECOND("s");

    private final String tag;

    private CaliperTimeUnit(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }
}
