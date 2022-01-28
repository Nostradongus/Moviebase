package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.model.Movie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class MovieRepository {

    @Autowired
    @Qualifier("node1Jdbc")
    private JdbcTemplate node1;

    @Autowired
    @Qualifier("node2Jdbc")
    private JdbcTemplate node2;

    @Autowired
    @Qualifier("node3Jdbc")
    private JdbcTemplate node3;

    public List<Movie> getAllMovies() {
        // get all movies from node 1 / central node
        List<Movie> movies = node1.query("SELECT * FROM movies", new MovieRowMapper());
        return movies;
    }

    public List<Movie> getAllMoviesBefore1980s() {
        // get all movies from node 2 (movies produced before 1980s)
        List<Movie> movies = node2.query("SELECT * FROM movies_b1980", new MovieRowMapper());
        return movies;
    }

    public List<Movie> getAllMoviesOnAndAfter1980s() {
        // get all movies from node 3 (movies produced on and after 1980s)
        List<Movie> movies = node3.query("SELECT * FROM movies_a1980", new MovieRowMapper());
        return movies;
    }

}

class MovieRowMapper implements RowMapper<Movie> {

    @Override
    public Movie mapRow(ResultSet rs, int rowNum) throws SQLException {
        Movie movie = new Movie();
        movie.setId(rs.getInt("id"));
        movie.setName(rs.getString("name"));
        movie.setYear(rs.getInt("year"));
        return movie;
    }

}
