package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.sql.SQLException;

public interface NodeRepository {

    void tryConnection() throws SQLException;

    Movie getMovieByUUID(String uuid) throws DataAccessException;

    Page<Movie> getMoviesByPage(Pageable pageable) throws DataAccessException;

    Page<Movie> searchMoviesByPage(Movie movie, Pageable page) throws DataAccessException;

    Page<Report> getMoviesPerGenreByPage(Pageable pageable) throws DataAccessException;

    Page<Report> getMoviesPerDirectorByPage(Pageable pageable) throws DataAccessException;

    Page<Report> getMoviesPerActorByPage(Pageable pageable) throws DataAccessException;

    Page<Report> getMoviesPerYearByPage(Pageable pageable) throws DataAccessException;

    void addMovie(Movie movie) throws DataAccessException;

    void updateMovie(Movie movie) throws DataAccessException;

    void deleteMovie(Movie movie) throws DataAccessException;

}
