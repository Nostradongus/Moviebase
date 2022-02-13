package com.stadvdb.group22.mco2.service;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.repository.Node1Repository;
import com.stadvdb.group22.mco2.repository.Node2Repository;
import com.stadvdb.group22.mco2.repository.Node3Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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

    // for node statuses during transactions that involve writing operations
    private final int OK = 0;
    private final int UNAVAILABLE = 1;
    private final int ERROR = 2;

    // distributed database re-sync switch
    private boolean resyncEnabled = true;

    public Movie getMovieByUUID (String uuid, int year) throws Exception {
        // disable distributed db re-sync before performing operation
        toggleResyncDB();
        Movie movie = null;
        // retrieve data from node 2 or 3 depending on year of movie
        if (year < 1980) {
            // try connection to node 2
            try {
                node2Repo.tryConnection();
                System.out.println("getMovieByUUID - Reading and retrieving data from node 2...");
                movie = node2Repo.getMovieByUUID(uuid);
                // enable resync db
                toggleResyncDB();
                return movie;
            } catch (Exception e) {
                // node 2 is currently down
                System.out.println("getMovieByUUID - Node 2 is currently down...");
            }
        } else {
            // try connection to node 3
            try {
                node3Repo.tryConnection();
                System.out.println("getMovieByUUID - Reading and retrieving data from node 3...");
                movie = node3Repo.getMovieByUUID(uuid);
                // enable resync db
                toggleResyncDB();
                return movie;
            } catch (Exception e) {
                // node 3 is currently down
                System.out.println("getMovieByUUID - Node 3 is currently down...");
            }
        }

        // try connection to node 1 instead
        try {
            node1Repo.tryConnection();
            System.out.println("getMovieByUUID - Reading and retrieving data from node 1...");
            movie = node1Repo.getMovieByUUID(uuid);
            // enable resync db
            toggleResyncDB();
            return movie;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMovieByUUID - Node 1 is currently down. Can't retrieve data, throwing exception...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public Page<Movie> getMoviesByPage(int page, int size) throws Exception {
        // disable resync db
        toggleResyncDB();

        // try connection to node 1 (central node)
        try {
            Page<Movie> movies = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesByPage - Reading and retrieving data from node 1...");
            movies = node1Repo.getMoviesByPage(PageRequest.of(page, size));
            // enable resync db
            toggleResyncDB();
            return movies;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMoviesByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        try {
            // try connection to both node 2 and 3
            node2Repo.tryConnection();
            node3Repo.tryConnection();
            // both nodes are available, perform data retrieval process
            int total = 0;
            List<Movie> movies = null;
            System.out.println("getMoviesByPage - Reading and retrieving data from node 2...");
            total += node2Repo.getNumOfMovies();
            movies = node2Repo.getMovies();
            System.out.println("getMoviesByPage - Reading and retrieving data from node 3...");
            total += node3Repo.getNumOfMovies();
            movies.addAll(node3Repo.getMovies());
            // get specific movies for page according to page number and size
            List<Movie> moviesPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                moviesPage.add(movies.get(i));
            }
            // enable resync db
            toggleResyncDB();
            return new PageImpl<>(moviesPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            System.out.println("getMoviesByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public Page<Movie> searchMoviesByPage(Movie movie, int page, int size) throws Exception {
        // disable db resync
        toggleResyncDB();
        Page<Movie> movies = null;
        // retrieve data from node 2 or 3 depending on year (if inputted)
        if (movie.getYear() != null) {
            // retrieve data from node 2 if year < 1980
            if (movie.getYear() < 1980) {
                // try connection to node 2
                try {
                    node2Repo.tryConnection();
                    System.out.println("searchMoviesByPage - Reading and retrieving data from node 2...");
                    movies = node2Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
                    // enable resync db
                    toggleResyncDB();
                    return movies;
                } catch (Exception e) {
                    // node 2 is currently down
                    System.out.println("searchMoviesByPage - Node 2 is currently down...");
                }
            } else {
                // try connection to node 3
                try {
                    node3Repo.tryConnection();
                    System.out.println("searchMoviesByPage - Reading and retrieving data from node 3...");
                    movies = node3Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
                    // enable resync db
                    toggleResyncDB();
                    return movies;
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
            movies = node1Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
            // enable resync db
            toggleResyncDB();
            return movies;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("searchMoviesByPage - Node 1 is currently down...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public Page<Report> getMoviesPerGenreByPage(int page, int size) throws Exception {
        // disable resync db
        toggleResyncDB();

        // try connection to node 1 (central node)
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerGenreByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerGenreByPage(PageRequest.of(page, size));
            // enable resync db
            toggleResyncDB();
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMoviesPerGenreByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        try {
            // try connection to both node 2 and 3
            node2Repo.tryConnection();
            node3Repo.tryConnection();
            // both nodes are available, perform data retrieval process
            int total = 0;
            List<Report> reports = null;
            System.out.println("getMoviesPerGenreByPage - Reading and retrieving data from node 2...");
            total += node2Repo.getNumOfGenres();
            reports = node2Repo.getMoviesPerGenre();
            System.out.println("getMoviesPerGenreByPage - Reading and retrieving data from node 3...");
            total += node3Repo.getNumOfGenres();
            List<Report> node3Reports = node3Repo.getMoviesPerGenre();
            for (int i = 0; i < node3Reports.size(); i++) {
                // append report to list if it is not found on the current report list, else combine / add count
                boolean unique = true;
                for (int j = 0; j < reports.size() && unique; j++) {
                    if (reports.get(j).getLabel().equalsIgnoreCase(node3Reports.get(i).getLabel())) {
                        reports.get(j).setCount(reports.get(j).getCount() + node3Reports.get(i).getCount());
                        unique = false;
                    }
                }
                if (unique) {
                    reports.add(node3Reports.get(i));
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            // enable resync db
            toggleResyncDB();
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            System.out.println("getMoviesByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public Page<Report> getMoviesPerDirectorByPage(int page, int size) throws Exception {
        // disable resync db
        toggleResyncDB();

        // try connection to node 1 (central node)
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerDirectorByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerDirectorByPage(PageRequest.of(page, size));
            // enable resync db
            toggleResyncDB();
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMoviesPerDirectorByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        try {
            // try connection to both node 2 and 3
            node2Repo.tryConnection();
            node3Repo.tryConnection();
            // both nodes are available, perform data retrieval process
            int total = 0;
            List<Report> reports = null;
            System.out.println("getMoviesPerDirectorByPage - Reading and retrieving data from node 2...");
            total += node2Repo.getNumOfDirectors();
            reports = node2Repo.getMoviesPerDirector();
            System.out.println("getMoviesPerDirectorByPage - Reading and retrieving data from node 3...");
            total += node3Repo.getNumOfDirectors();
            List<Report> node3Reports = node3Repo.getMoviesPerDirector();
            for (int i = 0; i < node3Reports.size(); i++) {
                // append report to list if it is not found on the current report list, else combine / add count
                boolean unique = true;
                for (int j = 0; j < reports.size() && unique; j++) {
                    if (reports.get(j).getLabel().equalsIgnoreCase(node3Reports.get(i).getLabel())) {
                        reports.get(j).setCount(reports.get(j).getCount() + node3Reports.get(i).getCount());
                        unique = false;
                    }
                }
                if (unique) {
                    reports.add(node3Reports.get(i));
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            // enable resync db
            toggleResyncDB();
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            System.out.println("getMoviesPerDirectorByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public Page<Report> getMoviesPerActorByPage(int page, int size) throws Exception {
        // disable resync db
        toggleResyncDB();

        // try connection to node 1 (central node)
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerActorByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerActorByPage(PageRequest.of(page, size));
            // enable resync db
            toggleResyncDB();
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMoviesPerActorByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        try {
            // try connection to both node 2 and 3
            node2Repo.tryConnection();
            node3Repo.tryConnection();
            // both nodes are available, perform data retrieval process
            int total = 0;
            List<Report> reports = null;
            System.out.println("getMoviesPerActorByPage - Reading and retrieving data from node 2...");
            total += node2Repo.getNumOfActors();
            reports = node2Repo.getMoviesPerActor();
            System.out.println("getMoviesPerActorByPage - Reading and retrieving data from node 3...");
            total += node3Repo.getNumOfActors();
            List<Report> node3Reports = node3Repo.getMoviesPerActor();
            for (int i = 0; i < node3Reports.size(); i++) {
                // append report to list if it is not found on the current report list, else combine / add count
                boolean unique = true;
                for (int j = 0; j < reports.size() && unique; j++) {
                    if (reports.get(j).getLabel().equalsIgnoreCase(node3Reports.get(i).getLabel())) {
                        reports.get(j).setCount(reports.get(j).getCount() + node3Reports.get(i).getCount());
                        unique = false;
                    }
                }
                if (unique) {
                    reports.add(node3Reports.get(i));
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            // enable resync db
            toggleResyncDB();
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            System.out.println("getMoviesPerActorByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public Page<Report> getMoviesPerYearByPage(int page, int size) throws Exception {
        // disable resync db
        toggleResyncDB();

        // try connection to node 1 (central node)
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerYearByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerYearByPage(PageRequest.of(page, size));
            // enable resync db
            toggleResyncDB();
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            System.out.println("getMoviesPerYearByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        try {
            // try connection to both node 2 and 3
            node2Repo.tryConnection();
            node3Repo.tryConnection();
            // both nodes are available, perform data retrieval process
            int total = 0;
            List<Report> reports = null;
            System.out.println("getMoviesPerYearByPage - Reading and retrieving data from node 2...");
            total += node2Repo.getNumOfYears();
            reports = node2Repo.getMoviesPerYear();
            System.out.println("getMoviesPerYearByPage - Reading and retrieving data from node 3...");
            total += node3Repo.getNumOfYears();
            List<Report> node3Reports = node3Repo.getMoviesPerYear();
            for (int i = 0; i < node3Reports.size(); i++) {
                // append report to list if it is not found on the current report list, else combine / add count
                boolean unique = true;
                for (int j = 0; j < reports.size() && unique; j++) {
                    if (reports.get(j).getLabel().equalsIgnoreCase(node3Reports.get(i).getLabel())) {
                        reports.get(j).setCount(reports.get(j).getCount() + node3Reports.get(i).getCount());
                        unique = false;
                    }
                }
                if (unique) {
                    reports.add(node3Reports.get(i));
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            // enable resync db
            toggleResyncDB();
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            System.out.println("getMoviesPerYearByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            // enable resync db
            toggleResyncDB();
            throw e;
        }
    }

    public void addMovie(Movie movie) throws TransactionException {
        // STRATEGY: Insert new movie data to node 1 first then insert to node 2 or 3 depending on year of new movie
        // - If node 1 is down, continue writing operation to node 2 or 3
        // - If node 1 transaction is successful but node 2 or 3 transaction failed, check recent transaction log of each
        //   and redo operations if all node transaction logs stated "commit". If at least one node transaction log
        //   did not state "commit" then rollback operations on the other nodes that had "commit" transactions
        // - Let resyncDB() do the data replication process (re-syncing) to the nodes if database down problem occurs

        int node1Status = OK;
        int node2Status = OK;
        int node3Status = OK;

        // try connection to node 1 before inserting new movie data
        try {
            node1Repo.tryConnection();
            System.out.println("addMovie - Inserting new movie data to node 1...");
            node1Repo.addMovie(movie);
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("addMovie - Node 1 is currently down...");
            node1Status = UNAVAILABLE;
        } catch (TransactionException transactionException) {
            // error occurred during transaction, set for database recovery
            System.out.println("addMovie - Error occurred during transaction in node 1...");
            node1Status = ERROR;
        }

        // insert new data to node 2 or 3 as well depending on year of new movie
        if (movie.getYear() < 1980) {
            // try connection to node 2 before inserting new data
            try {
                node2Repo.tryConnection();
                System.out.println("addMovie - Inserting new movie data to node 2...");
                node2Repo.addMovie(movie);
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("addMovie - Node 2 is currently down...");
                node2Status = UNAVAILABLE;
            } catch (TransactionException transactionException) {
                // error occurred during transaction, set for database recovery
                System.out.println("addMovie - Error occurred during transaction in node 2...");
                node2Status = ERROR;
            }
        } else {
            // try connection to node 3 before inserting new data
            try {
                node3Repo.tryConnection();
                System.out.println("addMovie - Inserting new movie data to node 3...");
                node3Repo.addMovie(movie);
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("addMovie - Node 3 is currently down...");
                node3Status = UNAVAILABLE;
            } catch (TransactionException transactionException) {
                // error occurred during transaction, set for database recovery
                System.out.println("addMovie - Error occurred during transaction in node 3...");
                node3Status = ERROR;
            }
        }

        // TODO: implement database recovery
    }

    public void updateMovie(Movie movie) throws TransactionException {
        // STRATEGY: Update existing movie data to node 1 first then update to node 2 or 3 depending on year of new movie
        // - If node 1 is down, continue writing operation to node 2 or 3
        // - If node 1 transaction is successful but node 2 or 3 transaction failed, check recent transaction log of each
        //   and redo operations if all node transaction logs stated "commit". If at least one node transaction log
        //   did not state "commit" then rollback operations on the other nodes that had "commit" transactions
        // - Let resyncDB() do the data replication process (re-syncing) to the nodes if database down problem occurs

        int node1Status = OK;
        int node2Status = OK;
        int node3Status = OK;

        // try connection to node 1 before updating existing movie data
        try {
            node1Repo.tryConnection();
            System.out.println("updateMovie - Updating movie data on node 1...");
            node1Repo.updateMovie(movie);
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("updateMovie - Node 1 is currently down...");
            node1Status = UNAVAILABLE;
        } catch (TransactionException transactionException) {
            // error occurred during transaction, set for database recovery
            System.out.println("updateMovie - Error occurred during transaction in node 1...");
            node1Status = ERROR;
        }

        // update data to node 2 or 3 as well depending on year of new movie
        if (movie.getYear() < 1980) {
            // try connection to node 2 before updating existing data
            try {
                node2Repo.tryConnection();
                System.out.println("updateMovie - Updating movie data on node 2...");
                node2Repo.updateMovie(movie);
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("updateMovie - Node 2 is currently down...");
                node2Status = UNAVAILABLE;
            } catch (TransactionException transactionException) {
                // error occurred during transaction, set for database recovery
                System.out.println("updateMovie - Error occurred during transaction in node 2...");
                node2Status = ERROR;
            }
        } else {
            // try connection to node 3 before updating existing data
            try {
                node3Repo.tryConnection();
                System.out.println("updateMovie - Updating movie data on node 3...");
                node3Repo.updateMovie(movie);
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("updateMovie - Node 3 is currently down...");
                node3Status = UNAVAILABLE;
            } catch (TransactionException transactionException) {
                // error occurred during transaction, set for database recovery
                System.out.println("updateMovie - Error occurred during transaction in node 3...");
                node3Status = ERROR;
            }
        }

        // TODO: implement database recovery
    }

    public void deleteMovie(Movie movie) throws TransactionException {
        // STRATEGY: Delete existing movie data on node 1 first then deleting on node 2 or 3 depending on year of new movie
        // - If node 1 is down, continue writing operation to node 2 or 3
        // - If node 1 transaction is successful but node 2 or 3 transaction failed, check recent transaction log of each
        //   and redo operations if all node transaction logs stated "commit". If at least one node transaction log
        //   did not state "commit" then rollback operations on the other nodes that had "commit" transactions
        // - Let resyncDB() do the data replication process (re-syncing) to the nodes if database down problem occurs

        int node1Status = OK;
        int node2Status = OK;
        int node3Status = OK;

        // try connection to node 1 before deleting existing data
        try {
            node1Repo.tryConnection();
            System.out.println("deleteMovie - Deleting movie data on node 1...");
            node1Repo.deleteMovie(movie);
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("deleteMovie - Node 1 is currently down...");
            node1Status = UNAVAILABLE;
        } catch (TransactionException transactionException) {
            // error occurred during transaction, set for database recovery
            System.out.println("deleteMovie - Error occurred during transaction in node 1...");
            node1Status = ERROR;
        }

        // delete data on node 2 or 3 as well depending on year of new movie
        if (movie.getYear() < 1980) {
            // try connection to node 2 before deleting existing data
            try {
                node2Repo.tryConnection();
                System.out.println("deleteMovie - Deleting movie data on node 2...");
                node2Repo.deleteMovie(movie);
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("deleteMovie - Node 2 is currently down...");
                node2Status = UNAVAILABLE;
            } catch (TransactionException transactionException) {
                // error occurred during transaction, set for database recovery
                System.out.println("deleteMovie - Error occurred during transaction in node 2...");
                node2Status = ERROR;
            }
        } else {
            // try connection to node 3 before updating existing data
            try {
                node3Repo.tryConnection();
                System.out.println("deleteMovie - Deleting movie data on node 3...");
                node3Repo.deleteMovie(movie);
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("deleteMovie - Node 3 is currently down...");
                node3Status = UNAVAILABLE;
            } catch (TransactionException transactionException) {
                // error occurred during transaction, set for database recovery
                System.out.println("deleteMovie - Error occurred during transaction in node 3...");
                node3Status = ERROR;
            }
        }

        // TODO: implement database recovery
    }

    // executes every 20 seconds, performs re-syncing of distributed database in case if system failures previously
    // occurred, specifically unavailability of nodes
    @Scheduled(fixedRate = 20000)
    private void resyncDB() {
        // TODO: implement distributed DB resync-ing (database recovery)
        if (resyncEnabled) {
            System.out.println("Test resync...");
        }
    }

    private void toggleResyncDB() {
        resyncEnabled = !resyncEnabled;
    }

}
