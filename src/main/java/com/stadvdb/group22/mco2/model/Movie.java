package com.stadvdb.group22.mco2.model;

public class Movie {

    private Integer id;
    private String name;
    private Integer year;

    public Movie() {}

    public Movie(int id, String name, int year) {
        this.id = id;
        this.name = name;
        this.year = year;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getYear() {
        return this.year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

}
