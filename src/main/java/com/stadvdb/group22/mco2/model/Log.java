package com.stadvdb.group22.mco2.model;

import java.sql.Date;
import java.sql.Timestamp;

public class Log {

    private String uuid;
    private String op;
    private String movieUuid;
    private Integer movieYear;
    private Timestamp ts;

    public Log() {}

    public Log(String uuid, String op, String movieUuid, Integer movieYear, Timestamp ts) {
        this.uuid = uuid;
        this.op = op;
        this.movieUuid = movieUuid;
        this.movieYear = movieYear;
        this.ts = ts;
    }

    public Log(String uuid, String op, Timestamp timestamp) {
        this.uuid = uuid;
        this.op = op;
        this.ts = timestamp;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getMovieUuid() {
        return movieUuid;
    }

    public void setMovieUuid(String movieUuid) {
        this.movieUuid = movieUuid;
    }

    public Integer getMovieYear() {
        return movieYear;
    }

    public void setMovieYear(Integer movieYear) {
        this.movieYear = movieYear;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }
}
