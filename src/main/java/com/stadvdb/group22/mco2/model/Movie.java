package com.stadvdb.group22.mco2.model;

public class Movie {

    private String title;
    private Integer year;
    private String genre;
    private String director;
    private String actor1;
    private String actor2;
    private String uuid;

    public Movie() {}

    public Movie(String title, Integer year, String genre,
                 String director, String actor1, String actor2, String uuid) {
        this.title = title;
        this.year = year;
        this.genre = genre;
        this.director = director;
        this.actor1 = actor1;
        this.actor2 = actor2;
        this.uuid = uuid;
    }

    public Integer getYear() {
        return this.year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenre() {
        return genre;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getActor1() {
        return actor1;
    }

    public void setActor1(String actor1) {
        this.actor1 = actor1;
    }

    public String getActor2() {
        return actor2;
    }

    public void setActor2(String actor2) {
        this.actor2 = actor2;
    }
}
