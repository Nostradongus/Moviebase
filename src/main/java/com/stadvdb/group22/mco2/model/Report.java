package com.stadvdb.group22.mco2.model;

public class Report {

    private String label;
    private Integer count;

    public Report() {}

    public Report(String label, Integer count) {
        this.label = label;
        this.count = count;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

}
