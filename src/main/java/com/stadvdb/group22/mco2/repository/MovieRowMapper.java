package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.model.Movie;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MovieRowMapper implements RowMapper<Movie> {

    @Override
    public Movie mapRow(ResultSet rs, int rowNum) throws SQLException {
        Movie movie = new Movie();
        movie.setTitle(rs.getString("title"));
        movie.setYear(rs.getInt("yr"));
        movie.setGenre(rs.getString("genre"));
        movie.setDirector(rs.getString("director"));
        movie.setActor1(rs.getString("actor1"));
        movie.setActor2(rs.getString("actor2"));
        movie.setUuid(rs.getString("uuid"));
        return movie;
    }

}
