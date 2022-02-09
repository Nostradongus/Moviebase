package com.stadvdb.group22.mco2.service;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.repository.Node1Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.*;

@Service
public class MovieService {

    @Autowired
    private Node1Repository node1Repo;

    public Page<Movie> getMoviesByPage(int page, int size) throws TransactionException {
        return node1Repo.getMoviesByPage(PageRequest.of(page, size));
    }

    public Page<Movie> searchMoviesByPage(String year, String title, String genre,
                                    String actor, String director, int page, int size) throws TransactionException {
        return node1Repo.searchMoviesByPage(year, title, genre, actor, director, PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerGenreByPage(int page, int size) throws TransactionException {
        return node1Repo.getMoviesPerGenreByPage(PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerDirectorByPage(int page, int size) throws TransactionException {
        return node1Repo.getMoviesPerDirectorByPage(PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerActorByPage(int page, int size) throws TransactionException {
        return node1Repo.getMoviesPerActorByPage(PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerYearByPage(int page, int size) throws TransactionException {
        return node1Repo.getMoviesPerYearByPage(PageRequest.of(page, size));
    }

    public void addMovie(Movie movie) throws TransactionException {
        node1Repo.addMovie(movie);
    }

    public void updateMovie(Movie movie) throws TransactionException {
        node1Repo.updateMovie(movie);
    }

    public void deleteMovie(Movie movie) throws TransactionException {
        node1Repo.deleteMovie(movie);
    }

}
