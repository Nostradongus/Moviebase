package com.stadvdb.group22.mco2.service;

import com.fasterxml.jackson.databind.ser.std.StdKeySerializers;
import com.stadvdb.group22.mco2.config.DBConfig;
import com.stadvdb.group22.mco2.model.Log;
import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.repository.Node1Repository;
import com.stadvdb.group22.mco2.repository.Node2Repository;
import com.stadvdb.group22.mco2.repository.Node3Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.*;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import java.sql.Date;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DistributedDBService {

    // NODE TRANSACTION MANAGERS
    @Autowired
    @Qualifier("node1TxManager")
    private DataSourceTransactionManager node1TxManager;

    @Autowired
    @Qualifier("node2TxManager")
    private DataSourceTransactionManager node2TxManager;

    @Autowired
    @Qualifier("node3TxManager")
    private DataSourceTransactionManager node3TxManager;

    // NODE REPOSITORIES
    @Autowired
    private Node1Repository node1Repo;

    @Autowired
    private Node2Repository node2Repo;

    @Autowired
    private Node3Repository node3Repo;

    // for node statuses during transactions that involve writing operations
    private final int INIT = 0;
    private final int OK = 1;
    private final int UNAVAILABLE = 2;
    private final int ERROR = 3;

    // distributed database re-sync switch
    @Autowired
    @Qualifier("resyncEnabled")
    private Boolean resyncEnabled;

    // distributed database maintenance status (if in maintenance or not)
    @Autowired
    @Qualifier("maintenance")
    private Boolean maintenance;

    // node 1 status (if down or not)
    @Autowired
    @Qualifier("node1Down")
    private Boolean node1Down;

    // node 2 status (if down or not)
    @Autowired
    @Qualifier("node2Down")
    private Boolean node2Down;

    // node 3 status (if down or not)
    @Autowired
    @Qualifier("node3Down")
    private Boolean node3Down;

    // TODO: temporary, might be removed
    @Autowired
    private ReentrantLock lock;

    private DefaultTransactionDefinition initTransactionDef() {
        // initialize transaction definition for transactions
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(DBConfig.ISOLATION_LEVEL);
        definition.setTimeout(DBConfig.T_TIME_OUT);
        return definition;
    }

    public Movie getMovieByUUID (String uuid, int year) throws Exception {
        // disable distributed db re-sync before performing operation
        resyncEnabled = false;

        // initialize transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        Movie movie = null;
        // retrieve data from node 2 or 3 depending on year of movie
        if (year < 1980) {
            TransactionStatus status = node2TxManager.getTransaction(definition);
            // try connection to node 2
            try {
                node2Repo.tryConnection();
                System.out.println("getMovieByUUID - Reading and retrieving data from node 2...");
                movie = node2Repo.getMovieByUUID(uuid);
                node2TxManager.commit(status);
                return movie;
            } catch (Exception e) {
                // node 2 is currently down
                node2TxManager.rollback(status);
                node2Down = true; // indicate to server that node 2 is down, to perform re-sync once it is back online
                System.out.println("getMovieByUUID - Node 2 is currently down...");
            }
        } else {
            TransactionStatus status = node3TxManager.getTransaction(definition);
            // try connection to node 3
            try {
                node3Repo.tryConnection();
                System.out.println("getMovieByUUID - Reading and retrieving data from node 3...");
                movie = node3Repo.getMovieByUUID(uuid);
                node3TxManager.commit(status);
                return movie;
            } catch (Exception e) {
                // node 3 is currently down
                node3TxManager.rollback(status);
                node3Down = true;
                System.out.println("getMovieByUUID - Node 3 is currently down...");
            }
        }

        // try connection to node 1 instead
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            node1Repo.tryConnection();
            System.out.println("getMovieByUUID - Reading and retrieving data from node 1...");
            movie = node1Repo.getMovieByUUID(uuid);
            node1TxManager.commit(status);
            if (node2Down || node3Down) { // if node 2 or 3 down, enable db re-sync
                resyncEnabled = true;
            }
            return movie;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            node1Down = true;
            resyncEnabled = true;
            System.out.println("getMovieByUUID - Node 1 is currently down. Can't retrieve data, throwing exception...");
            throw e;
        }
    }

    public Page<Movie> getMoviesByPage(int page, int size) throws Exception {
        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            Page<Movie> movies = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesByPage - Reading and retrieving data from node 1...");
            movies = node1Repo.getMoviesByPage(PageRequest.of(page, size));
            node1TxManager.commit(status);
            return movies;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            node1Down = true;
            System.out.println("getMoviesByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        TransactionStatus node2Status = node2TxManager.getTransaction(definition);
        TransactionStatus node3Status = node3TxManager.getTransaction(definition);
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
            node2TxManager.commit(node2Status);
            node3TxManager.commit(node3Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(moviesPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node2TxManager.rollback(node2Status);
            node3TxManager.rollback(node3Status);
            try {
                node2Repo.tryConnection();
            } catch (Exception exception) {
                node2Down = true;
            }
            try {
                node3Repo.tryConnection();
            } catch (Exception exception) {
                node3Down = true;
            }
            resyncEnabled = true;
            System.out.println("getMoviesByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            throw e;
        }
    }

    public Page<Movie> searchMoviesByPage(Movie movie, int page, int size) throws Exception {
        // disable db resync
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        Page<Movie> movies = null;
        // retrieve data from node 2 or 3 depending on year (if inputted)
        if (movie.getYear() != null) {
            // retrieve data from node 2 if year < 1980
            if (movie.getYear() < 1980) {
                TransactionStatus status = node2TxManager.getTransaction(definition);
                // try connection to node 2
                try {
                    node2Repo.tryConnection();
                    System.out.println("searchMoviesByPage - Reading and retrieving data from node 2...");
                    movies = node2Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
                    node2TxManager.commit(status);
                    return movies;
                } catch (Exception e) {
                    // node 2 is currently down
                    node2TxManager.rollback(status);
                    node2Down = true;
                    System.out.println("searchMoviesByPage - Node 2 is currently down...");
                }
            } else {
                TransactionStatus status = node3TxManager.getTransaction(definition);
                // try connection to node 3
                try {
                    node3Repo.tryConnection();
                    System.out.println("searchMoviesByPage - Reading and retrieving data from node 3...");
                    movies = node3Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
                    node3TxManager.commit(status);
                    return movies;
                } catch (Exception e) {
                    // node 3 is currently down
                    node3TxManager.rollback(status);
                    node3Down = true;
                    System.out.println("searchMoviesByPage - Node 3 is currently down...");
                }
            }
        }

        // try connection to node 1 instead
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            node1Repo.tryConnection();
            System.out.println("searchMoviesByPage - Reading and retrieving data from node 1...");
            movies = node1Repo.searchMoviesByPage(movie, PageRequest.of(page, size));
            node1TxManager.commit(status);
            if (node2Down || node3Down) {
                resyncEnabled = true;
            }
            return movies;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            System.out.println("searchMoviesByPage - Node 1 is currently down...");
            node1Down = true;
            resyncEnabled = true;
            throw e;
        }
    }

    public Page<Report> getMoviesPerGenreByPage(int page, int size) throws Exception {
        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerGenreByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerGenreByPage(PageRequest.of(page, size));
            node1TxManager.commit(status);
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            node1Down = true;
            System.out.println("getMoviesPerGenreByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        TransactionStatus node2Status = node2TxManager.getTransaction(definition);
        TransactionStatus node3Status = node3TxManager.getTransaction(definition);
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
            node2TxManager.commit(node2Status);
            node3TxManager.commit(node3Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node2TxManager.rollback(node2Status);
            node3TxManager.rollback(node3Status);
            try {
                node2Repo.tryConnection();
            } catch (Exception exception) {
                node2Down = true;
            }
            try {
                node3Repo.tryConnection();
            } catch (Exception exception) {
                node3Down = true;
            }
            resyncEnabled = true;
            System.out.println("getMoviesByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            throw e;
        }
    }

    public Page<Report> getMoviesPerDirectorByPage(int page, int size) throws Exception {
        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerDirectorByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerDirectorByPage(PageRequest.of(page, size));
            node1TxManager.commit(status);
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            node1Down = true;
            System.out.println("getMoviesPerDirectorByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        TransactionStatus node2Status = node2TxManager.getTransaction(definition);
        TransactionStatus node3Status = node3TxManager.getTransaction(definition);
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
            node2TxManager.commit(node2Status);
            node3TxManager.commit(node3Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node2TxManager.rollback(node2Status);
            node3TxManager.rollback(node3Status);
            try {
                node2Repo.tryConnection();
            } catch (Exception exception) {
                node2Down = true;
            }
            try {
                node3Repo.tryConnection();
            } catch (Exception exception) {
                node3Down = true;
            }
            resyncEnabled = true;
            System.out.println("getMoviesPerDirectorByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            throw e;
        }
    }

    public Page<Report> getMoviesPerActorByPage(int page, int size) throws Exception {
        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerActorByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerActorByPage(PageRequest.of(page, size));
            node1TxManager.commit(status);
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            node1Down = true;
            System.out.println("getMoviesPerActorByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        TransactionStatus node2Status = node2TxManager.getTransaction(definition);
        TransactionStatus node3Status = node3TxManager.getTransaction(definition);
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
            node2TxManager.commit(node2Status);
            node3TxManager.commit(node3Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node2TxManager.rollback(node2Status);
            node3TxManager.rollback(node3Status);
            try {
                node2Repo.tryConnection();
            } catch (Exception exception) {
                node2Down = true;
            }
            try {
                node3Repo.tryConnection();
            } catch (Exception exception) {
                node3Down = true;
            }
            resyncEnabled = true;
            System.out.println("getMoviesPerActorByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            throw e;
        }
    }

    public Page<Report> getMoviesPerYearByPage(int page, int size) throws Exception {
        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            Page<Report> reports = null;
            node1Repo.tryConnection();
            System.out.println("getMoviesPerYearByPage - Reading and retrieving data from node 1...");
            reports = node1Repo.getMoviesPerYearByPage(PageRequest.of(page, size));
            node1TxManager.commit(status);
            return reports;
        } catch (Exception e) {
            // node 1 is currently down
            node1TxManager.rollback(status);
            node1Down = true;
            System.out.println("getMoviesPerYearByPage - Node 1 is currently down...");
        }

        // try connection to both node 2 and 3, if at least one is down then cannot perform data retrieval process and throw exception
        // both node 2 and 3 needs to be available to combine and replicate node 1 database for this specific query
        TransactionStatus node2Status = node2TxManager.getTransaction(definition);
        TransactionStatus node3Status = node3TxManager.getTransaction(definition);
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
            node2TxManager.commit(node2Status);
            node3TxManager.commit(node3Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node2TxManager.rollback(node2Status);
            node3TxManager.rollback(node3Status);
            try {
                node2Repo.tryConnection();
            } catch (Exception exception) {
                node2Down = true;
            }
            try {
                node3Repo.tryConnection();
            } catch (Exception exception) {
                node3Down = true;
            }
            resyncEnabled = true;
            System.out.println("getMoviesPerYearByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            throw e;
        }
    }

    private Timestamp getCurrTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public void addMovie(Movie movie) throws Exception {
        // STRATEGY: Insert new movie data to node 1 first then insert to node 2 or 3 depending on year of new movie
        // - If node 1 is down, continue writing operation to node 2 or 3
        // - If node 1 transaction is successful but node 2 or 3 transaction failed, check recent transaction log of each
        //   and redo operations if all node transaction logs stated "commit". If at least one node transaction log
        //   did not state "commit" then rollback operations on the other nodes that had "commit" transactions
        // - Let resyncDB() do the data replication process (re-syncing) to the nodes if database down problem occurs

        // disable db resync
        resyncEnabled = false;

        // node statuses
        int node1Status = INIT;
        int node2Status = INIT;
        int node3Status = INIT;

        // node logs
        List<Log> node1Logs = new ArrayList<>();
        List<Log> node2Logs = new ArrayList<>();
        List<Log> node3Logs = new ArrayList<>();

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // node transaction statuses
        TransactionStatus node1TxStatus = node1TxManager.getTransaction(definition);
        TransactionStatus node2TxStatus = node2TxManager.getTransaction(definition);
        TransactionStatus node3TxStatus = node3TxManager.getTransaction(definition);

        // generate new transaction uuid
        String tUuid = UUID.randomUUID().toString();

        // try connection to node 1 before inserting new movie data
        try {
            node1Repo.tryConnection();
            System.out.println("addMovie - Inserting new movie data to node 1...");
            // start transaction
            node1Logs.add(new Log(tUuid, "START", getCurrTimestamp()));
            // log before write
            node1Logs.add(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
            node1Repo.addMovie(movie);
            // log before commit
            node1Logs.add(new Log(tUuid, "COMMIT", getCurrTimestamp()));
            node1Status = OK; // transaction is ready for commit
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("addMovie - Node 1 is currently down...");
            node1Status = UNAVAILABLE;
        } catch (Exception exception) {
            System.out.println("addMovie - Error occurred during transaction in node 1...");
            node1Status = ERROR;
        }

        // insert new data to node 2 or 3 as well depending on year of new movie
        if (movie.getYear() < 1980) {
            // try connection to node 2 before inserting new data
            try {
                node2Repo.tryConnection();
                System.out.println("addMovie - Inserting new movie data to node 2...");
                node2Logs.add(new Log(tUuid, "START", getCurrTimestamp()));
                node2Logs.add(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                node2Repo.addMovie(movie);
                node2Logs.add(new Log(tUuid, "COMMIT", getCurrTimestamp()));
                node2Status = OK;
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("addMovie - Node 2 is currently down...");
                node2Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("addMovie - Error occurred during transaction in node 2...");
                node2Status = ERROR;
            }
        } else {
            // try connection to node 3 before inserting new data
            try {
                node3Repo.tryConnection();
                System.out.println("addMovie - Inserting new movie data to node 3...");
                node3Logs.add(new Log(tUuid, "START", getCurrTimestamp()));
                node3Logs.add(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                node3Repo.addMovie(movie);
                node3Logs.add(new Log(tUuid, "COMMIT", getCurrTimestamp()));
                node3Status = OK;
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("addMovie - Node 3 is currently down...");
                node3Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("addMovie - Error occurred during transaction in node 3...");
                node3Status = ERROR;
            }
        }

        // DATABASE RECOVERY
        // if nodes' transactions are ready to commit
        node1TxManager.rollback(node1TxStatus);
        node2TxManager.rollback(node2TxStatus);
    }

    public void updateMovie(Movie movie) throws Exception {
        // STRATEGY: Update existing movie data to node 1 first then update to node 2 or 3 depending on year of new movie
        // - If node 1 is down, continue writing operation to node 2 or 3
        // - If node 1 transaction is successful but node 2 or 3 transaction failed, check recent transaction log of each
        //   and redo operations if all node transaction logs stated "commit". If at least one node transaction log
        //   did not state "commit" then rollback operations on the other nodes that had "commit" transactions
        // - Let resyncDB() do the data replication process (re-syncing) to the nodes if database down problem occurs

        // disable resync db
        toggleResyncDB();

        int node1Status = OK;
        int node2Status = OK;
        int node3Status = OK;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 before updating existing movie data
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            node1Repo.tryConnection();
            System.out.println("updateMovie - Updating movie data on node 1...");
            node1Repo.updateMovie(movie);
            node1TxManager.commit(status);
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("updateMovie - Node 1 is currently down...");
            node1Status = UNAVAILABLE;
        } catch (Exception exception) {
            // error occurred during transaction, set for database recovery
            node1TxManager.rollback(status);
            System.out.println("updateMovie - Error occurred during transaction in node 1...");
            node1Status = ERROR;
        }

        // update data to node 2 or 3 as well depending on year of new movie
        if (movie.getYear() < 1980) {
            status = node2TxManager.getTransaction(definition);
            // try connection to node 2 before updating existing data
            try {
                node2Repo.tryConnection();
                System.out.println("updateMovie - Updating movie data on node 2...");
                node2Repo.updateMovie(movie);
                node2TxManager.commit(status);
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("updateMovie - Node 2 is currently down...");
                node2Status = UNAVAILABLE;
            } catch (Exception exception) {
                // error occurred during transaction, set for database recovery
                node2TxManager.rollback(status);
                System.out.println("updateMovie - Error occurred during transaction in node 2...");
                node2Status = ERROR;
            }
        } else {
            status = node3TxManager.getTransaction(definition);
            // try connection to node 3 before updating existing data
            try {
                node3Repo.tryConnection();
                System.out.println("updateMovie - Updating movie data on node 3...");
                node3Repo.updateMovie(movie);
                node3TxManager.commit(status);
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("updateMovie - Node 3 is currently down...");
                node3Status = UNAVAILABLE;
            } catch (Exception exception) {
                // error occurred during transaction, set for database recovery
                node3TxManager.rollback(status);
                System.out.println("updateMovie - Error occurred during transaction in node 3...");
                node3Status = ERROR;
            }
        }

        // TODO: implement database recovery

        // enable db resync
        toggleResyncDB();
    }

    public void deleteMovie(Movie movie) throws Exception {
        // STRATEGY: Delete existing movie data on node 1 first then deleting on node 2 or 3 depending on year of new movie
        // - If node 1 is down, continue writing operation to node 2 or 3
        // - If node 1 transaction is successful but node 2 or 3 transaction failed, check recent transaction log of each
        //   and redo operations if all node transaction logs stated "commit". If at least one node transaction log
        //   did not state "commit" then rollback operations on the other nodes that had "commit" transactions
        // - Let resyncDB() do the data replication process (re-syncing) to the nodes if database down problem occurs

        // disable resync db
        toggleResyncDB();

        int node1Status = OK;
        int node2Status = OK;
        int node3Status = OK;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 before deleting existing data
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            node1Repo.tryConnection();
            System.out.println("deleteMovie - Deleting movie data on node 1...");
            node1Repo.deleteMovie(movie);
            node1TxManager.commit(status);
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("deleteMovie - Node 1 is currently down...");
            node1Status = UNAVAILABLE;
        } catch (Exception exception) {
            // error occurred during transaction, set for database recovery
            node1TxManager.rollback(status);
            System.out.println("deleteMovie - Error occurred during transaction in node 1...");
            node1Status = ERROR;
        }

        // delete data on node 2 or 3 as well depending on year of new movie
        if (movie.getYear() < 1980) {
            status = node2TxManager.getTransaction(definition);
            // try connection to node 2 before deleting existing data
            try {
                node2Repo.tryConnection();
                System.out.println("deleteMovie - Deleting movie data on node 2...");
                node2Repo.deleteMovie(movie);
                node2TxManager.commit(status);
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("deleteMovie - Node 2 is currently down...");
                node2Status = UNAVAILABLE;
            } catch (Exception exception) {
                // error occurred during transaction, set for database recovery
                node2TxManager.rollback(status);
                System.out.println("deleteMovie - Error occurred during transaction in node 2...");
                node2Status = ERROR;
            }
        } else {
            status = node3TxManager.getTransaction(definition);
            // try connection to node 3 before updating existing data
            try {
                node3Repo.tryConnection();
                System.out.println("deleteMovie - Deleting movie data on node 3...");
                node3Repo.deleteMovie(movie);
                node3TxManager.commit(status);
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("deleteMovie - Node 3 is currently down...");
                node3Status = UNAVAILABLE;
            } catch (Exception exception) {
                // error occurred during transaction, set for database recovery
                node3TxManager.rollback(status);
                System.out.println("deleteMovie - Error occurred during transaction in node 3...");
                node3Status = ERROR;
            }
        }

        // TODO: implement database recovery

        // enable resync db
        toggleResyncDB();
    }

    // executes every 10 seconds, performs re-syncing of distributed database in case if system failures previously
    // occurred, specifically unavailability of nodes
    @Scheduled(fixedRate = 10000)
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
