package com.stadvdb.group22.mco2.service;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.repository.Node1Repository;
import com.stadvdb.group22.mco2.repository.Node2Repository;
import com.stadvdb.group22.mco2.repository.Node3Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.*;

import java.util.concurrent.locks.ReentrantLock;

@Service
public class DistributedDBService {

    @Autowired
    private Node1Repository node1Repo;

    @Autowired
    private Node2Repository node2Repo;

    @Autowired
    private Node3Repository node3Repo;

    @Autowired
    private ReentrantLock lock;

    public Movie getMovieByUUID (String uuid, int year) throws Exception {
        // retrieve data from node 2 or 3 depending on year of movie
        if (year < 1980) {
            // try connection to node 2
            try {
                node2Repo.tryConnection();
                System.out.println("getMovieByUUID - Reading and retrieving data from node 2...");
                return node2Repo.getMovieByUUID(uuid);
            } catch (Exception e) {
                // node 2 is currently down
                System.out.println("getMovieByUUID - Node 2 is currently down...");
            }
        } else {
            // try connection to node 3
            try {
                node3Repo.tryConnection();
                System.out.println("getMovieByUUID - Reading and retrieving data from node 3...");
                return node3Repo.getMovieByUUID(uuid);
            } catch (Exception e) {
                // node 3 is currently down
                System.out.println("getMovieByUUID - Node 3 is currently down...");
            }
        }

        // try connection to node 1 instead
        try {
            node1Repo.tryConnection();
            System.out.println("getMovieByUUID - Reading and retrieving data from node 1...");
            return node1Repo.getMovieByUUID(uuid);
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMovieByUUID - Node 1 is currently down...");
            System.out.println("getMovieByUUID - All nodes are currently down...");
            throw e;
        }
    }

    public Page<Movie> getMoviesByPage(int page, int size) {
        Page<Movie> movies = null;

        // try connection to node 1 (central node)
        try {
            node1Repo.tryConnection();
            movies = node1Repo.getMoviesByPage(PageRequest.of(page, size));
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMoviesByPage - Node 1 is currently down...");
        }

        return movies;
    }

    public Page<Movie> searchMoviesByPage(Movie movie, int page, int size) throws Exception {
        // retrieve data from node 2 or 3 depending on year (if inputted)
        if (movie.getYear() != null) {
            // retrieve data from node 2 if year < 1980
            if (movie.getYear() < 1980) {
                // try connection to node 2
                try {
                    node2Repo.tryConnection();
                    System.out.println("searchMoviesByPage - Reading and retrieving data from node 2...");
                    return node2Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
                } catch (Exception e) {
                    // node 2 is currently down
                    System.out.println("searchMoviesByPage - Node 2 is currently down...");
                }
            } else {
                // try connection to node 3
                try {
                    node3Repo.tryConnection();
                    System.out.println("searchMoviesByPage - Reading and retrieving data from node 3...");
                    return node3Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
                } catch (Exception e) {
                    // node 3 is currently down
                    System.out.println("searchMoviesByPage - Node 3 is currently down...");
                }
            }
        }

        // try connection to node 1 instead
        try {
            node1Repo.tryConnection();
            System.out.println("searchMoviesByPage - Reading and retrieving data from node 1...");
            return node1Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("searchMoviesByPage - Node 1 is currently down...");
            System.out.println("searchMoviesByPage - All nodes are currently down...");
            throw e;
        }
    }

    public Page<Report> getMoviesPerGenreByPage(int page, int size) {
        return node1Repo.getMoviesPerGenreByPage(PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerDirectorByPage(int page, int size) {
        return node1Repo.getMoviesPerDirectorByPage(PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerActorByPage(int page, int size) {
        return node1Repo.getMoviesPerActorByPage(PageRequest.of(page, size));
    }

    public Page<Report> getMoviesPerYearByPage(int page, int size) {
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

    // executes every 10 seconds
    @Scheduled(fixedRate = 10000)
    public void resyncDB() {
        // TODO: implement
        System.out.println("Test resync...");
    }

}
