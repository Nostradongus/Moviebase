package com.stadvdb.group22.mco2.service;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.repository.MovieRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MovieService {

    @Autowired
    private MovieRepository movieRepository;

    public List<Movie> getAllMovies() {
        return movieRepository.getAllMovies();
    }

    // TODO: temporary, to be removed
    public List<Movie> getAllMoviesBefore1980s() {
        return movieRepository.getAllMoviesBefore1980s();
    }

    // TODO: temporary, to be removed
    public List<Movie> getAllMoviesOnAndAfter1980s() {
        return movieRepository.getAllMoviesOnAndAfter1980s();
    }

}
