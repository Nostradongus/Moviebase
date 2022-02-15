package com.stadvdb.group22.mco2.service;

import com.fasterxml.jackson.databind.ser.std.StdKeySerializers;
import com.stadvdb.group22.mco2.config.DBConfig;
import com.stadvdb.group22.mco2.exception.TransactionErrorException;
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
import java.util.concurrent.TimeUnit;
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

    // for initial check if need for re-sync (always done at start of server)
    @Autowired
    @Qualifier("initial")
    private Boolean initial;

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

    // TODO: temporary (not used), might be removed
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
        // if at maintenance, cancel operation and inform user
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable distributed db re-sync before performing operation
        resyncEnabled = false;

        // initialize transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        Movie movie = null;
        // retrieve data from node 2 or 3 depending on year of movie, and node 2 or 3 is not down
        if (year < 1980) {
            TransactionStatus status = node2TxManager.getTransaction(definition);
            // try connection to node 2
            try {
                if (node2Down) {
                    throw new Exception ();
                }
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
        } else if (year >= 1980) {
            TransactionStatus status = node3TxManager.getTransaction(definition);
            // try connection to node 3
            try {
                if (node3Down) {
                    throw new Exception ();
                }
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
            // if node 1 was down before, do not retrieve data as it may have inconsistent data
            if (node1Down) {
                throw new Exception();
            }
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
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            if (node1Down) {
                throw new Exception();
            }
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
            // if node 2 or 3 down from previous transactions, dont retrieve data as at least one node may contain inconsistent data
            if (node2Down || node3Down) {
                throw new Exception();
            }
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
            node3TxManager.commit(node3Status);
            node2TxManager.commit(node2Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(moviesPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node3TxManager.rollback(node3Status);
            node2TxManager.rollback(node2Status);
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
            System.out.println("getMoviesPerGenreByPage - Node 2 or 3 is currently down. Cannot retrieve data, exception thrown...");
            throw e;
        }
    }

    public Page<Movie> searchMoviesByPage(Movie movie, int page, int size) throws Exception {
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

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
                    if (node2Down) {
                        throw new Exception ();
                    }
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
            } else if (movie.getYear() >= 1980) {
                TransactionStatus status = node3TxManager.getTransaction(definition);
                // try connection to node 3
                try {
                    if (node3Down) {
                        throw new Exception ();
                    }
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
            if (node1Down) {
                throw new Exception();
            }
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
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            if (node1Down) {
                throw new Exception();
            }
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
            if (node2Down || node3Down) {
                throw new Exception ();
            }
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
                } else {
                    total -= 1;
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            node3TxManager.commit(node3Status);
            node2TxManager.commit(node2Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node3TxManager.rollback(node3Status);
            node2TxManager.rollback(node2Status);
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
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            if (node1Down) {
                throw new Exception();
            }
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
            if (node2Down || node3Down) {
                throw new Exception();
            }
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
                } else {
                    total -= 1;
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            node3TxManager.commit(node3Status);
            node2TxManager.commit(node2Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node3TxManager.rollback(node3Status);
            node2TxManager.rollback(node2Status);
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
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            if (node1Down) {
                throw new Exception ();
            }
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
            if (node2Down || node3Down) {
                throw new Exception ();
            }
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
                } else {
                    total -= 1;
                }
            }
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            node3TxManager.commit(node3Status);
            node2TxManager.commit(node2Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node3TxManager.rollback(node3Status);
            node2TxManager.rollback(node2Status);
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
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable resync db
        resyncEnabled = false;

        // transaction definition
        DefaultTransactionDefinition definition = initTransactionDef();

        // try connection to node 1 (central node)
        TransactionStatus status = node1TxManager.getTransaction(definition);
        try {
            if (node1Down) {
                throw new Exception ();
            }
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
            if (node2Down || node3Down) {
                throw new Exception ();
            }
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
            reports.addAll(node3Repo.getMoviesPerYear());
            // get specific reports according to page number and size
            List<Report> reportsPage = new ArrayList<>();
            for (int i = page * size; i < page * size + size; i++) {
                reportsPage.add(reports.get(i));
            }
            node3TxManager.commit(node3Status);
            node2TxManager.commit(node2Status);
            if (node1Down) {
                resyncEnabled = true;
            }
            return new PageImpl<>(reportsPage, PageRequest.of(page, size), total);
        } catch (Exception e) {
            // node 2 or 3 is down, cannot perform data retrieval so throw exception
            node3TxManager.rollback(node3Status);
            node2TxManager.rollback(node2Status);
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
        // RECOVERY METHOD: Deferred Modification
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable db resync
        resyncEnabled = false;

        // node statuses
        int node1Status = INIT;
        int node2Status = INIT;
        int node3Status = INIT;
        // node transaction statuses (by order of commit / rollback, node2 / node3 then node1)
        TransactionStatus node1TxStatus = null;
        TransactionStatus node2TxStatus = null;
        TransactionStatus node3TxStatus = null;

        // generate new transaction uuid
        String tUuid = UUID.randomUUID().toString();

        node1TxStatus = node1TxManager.getTransaction(initTransactionDef());
        try {
            if (node1Down) {
                throw new SQLException ();
            }
            // try connection to node 1 before inserting new movie data
            node1Repo.tryConnection();
            System.out.println("addMovie - Inserting new movie data to node 1...");
            node1Repo.addMovie(movie);
            node1Status = OK; // transaction is ready for commit
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("addMovie - Node 1 is currently down...");
            node1TxManager.rollback(node1TxStatus);
            node1Down = true;
            node1Status = UNAVAILABLE;
        } catch (Exception exception) {
            // transaction error during insertion, rollback and don't redo
            System.out.println("addMovie - Error occurred during transaction in node 1...");
            node1TxManager.rollback(node1TxStatus);
            node1Status = ERROR;
        }

        // if node 1 transaction ready for commit or is unavailable
        if ((node1Status == OK || node1Status == UNAVAILABLE) && movie.getYear() < 1980) {
            node2TxStatus = node2TxManager.getTransaction(initTransactionDef());
            try {
                if (node2Down) {
                    throw new SQLException ();
                }
                // try connection to node 2 before inserting new data
                node2Repo.tryConnection();
                System.out.println("addMovie - Inserting new movie data to node 2...");
                node2Repo.addMovie(movie);
                node2Status = OK;
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("addMovie - Node 2 is currently down...");
                node2TxManager.rollback(node2TxStatus);
                node2Down = true;
                node2Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("addMovie - Error occurred during transaction in node 2...");
                node2TxManager.rollback(node2TxStatus);
                node2Status = ERROR;
            }
        } else if ((node1Status == OK || node1Status == UNAVAILABLE) && movie.getYear() >= 1980) {
            node3TxStatus = node3TxManager.getTransaction(initTransactionDef());
            try {
                if (node3Down) {
                    throw new SQLException ();
                }
                // try connection to node 3 before inserting new data
                node3Repo.tryConnection();
                System.out.println("addMovie - Inserting new movie data to node 3...");
                node3Repo.addMovie(movie);
                node3Status = OK;
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("addMovie - Node 3 is currently down...");
                node3TxManager.rollback(node3TxStatus);
                node3Down = true;
                node3Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("addMovie - Error occurred during transaction in node 3...");
                node3TxManager.rollback(node3TxStatus);
                node3Status = ERROR;
            }
        } else {
            // else, an error has occurred and user has to try the query again in the UI
            System.out.println("addMovie - Cannot perform data retrieval, throwing exception...");
            throw new TransactionErrorException();
        }

        // if node 2 or 3 failed, rollback node 1 transaction and indicate user to try the query again
        if (node2Status == ERROR || node3Status == ERROR) {
            node1TxManager.rollback(node1TxStatus);
            node1Status = ERROR;
            System.out.println("addMovie - Cannot perform data retrieval, throwing exception...");
            throw new TransactionErrorException();
        }

        // if both nodes are ready for commit
        if (node1Status == OK || node1Status == UNAVAILABLE) {
            if (node2Status == OK) {
                // try node 2 commit transaction
                try {
                    // add transaction log
                    node2Repo.addLog(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node2TxManager.commit(node2TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node2TxManager.rollback(node2TxStatus);
                    node2Status = ERROR;
                }
            } else if (node3Status == OK) {
                // try node 3 commit transaction
                try {
                    // add transaction log
                    node3Repo.addLog(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node3TxManager.commit(node3TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node3TxManager.rollback(node3TxStatus);
                    node3Status = ERROR;
                }
            }
            // try node 1 commit transaction
            if (node1Status == OK) {
                try {
                    node1Repo.addLog(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node1TxManager.commit(node1TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node1TxManager.rollback(node1TxStatus);
                    node1Status = ERROR;
                }
            }
            // recover if node 2 failed
            while (node2Status == ERROR) {
                node2TxStatus = node2TxManager.getTransaction(initTransactionDef());
                try {
                    // node 2 might be down, try connection
                    node2Repo.tryConnection();
                    node2Repo.addMovie(movie);
                    node2Repo.addLog(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node2TxManager.commit(node2TxStatus);
                    node2Status = OK; // node 2 now recovered
                } catch (SQLException sqlException) {
                    node2TxManager.rollback(node2TxStatus);
                    node2Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node2TxManager.rollback(node2TxStatus);
                }
            }
            // recover if node 3 failed
            while (node3Status == ERROR) {
                node3TxStatus = node3TxManager.getTransaction(initTransactionDef());
                try {
                    // node 2 might be down, try connection
                    node3Repo.tryConnection();
                    node3Repo.addMovie(movie);
                    node3Repo.addLog(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node3TxManager.commit(node3TxStatus);
                    node3Status = OK; // node 2 now recovered
                } catch (SQLException sqlException) {
                    node3TxManager.rollback(node3TxStatus);
                    node3Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node3TxManager.rollback(node3TxStatus);
                }
            }
            // recover if node 1 failed
            while (node1Status == ERROR) {
                node1TxStatus = node1TxManager.getTransaction(initTransactionDef());
                try {
                    // node 1 might be down, try connection
                    node1Repo.tryConnection();
                    node1Repo.addMovie(movie);
                    node1Repo.addLog(new Log(tUuid, "INSERT", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node1TxManager.commit(node1TxStatus);
                    node1Status = OK; // node 1 now recovered
                } catch (SQLException sqlException) {
                    node1TxManager.rollback(node1TxStatus);
                    node1Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node1TxManager.rollback(node1TxStatus);
                }
            }
        }
        // if nodes suddenly went down or unavailable, signal server for db re-sync
        if (node1Status == UNAVAILABLE) {
            node1Down = true;
            resyncEnabled = true;
        }
        if (node2Status == UNAVAILABLE) {
            node2Down = true;
            resyncEnabled = true;
        }
        if (node3Status == UNAVAILABLE) {
            node3Down = true;
            resyncEnabled = true;
        }
        // operation cannot be done as both nodes are unavailable, throw exception for user to try again the query later
        if (node1Status == UNAVAILABLE && (node2Status == UNAVAILABLE || node3Status == UNAVAILABLE)) {
            System.out.println("addMovie - Nodes are down, throwing exception...");
            throw new Exception ();
        }
    }

    public void updateMovie(Movie movie) throws Exception {
        // STRATEGY: Update existing movie data to node 1 first then update to node 2 or 3 depending on year of new movie
        // RECOVERY METHOD: Deferred Modification
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable db resync
        resyncEnabled = false;

        // node statuses
        int node1Status = INIT;
        int node2Status = INIT;
        int node3Status = INIT;
        // node transaction statuses (by order of commit / rollback, node2 / node3 then node1)
        TransactionStatus node1TxStatus = null;
        TransactionStatus node2TxStatus = null;
        TransactionStatus node3TxStatus = null;

        // generate new transaction uuid
        String tUuid = UUID.randomUUID().toString();

        node1TxStatus = node1TxManager.getTransaction(initTransactionDef());
        try {
            if (node1Down) {
                throw new SQLException();
            }
            // try connection to node 1 before updating movie data
            node1Repo.tryConnection();
            System.out.println("updateMovie - Updating movie data to node 1...");
            node1Repo.updateMovie(movie);
            node1Status = OK; // transaction is ready for commit
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("updateMovie - Node 1 is currently down...");
            node1TxManager.rollback(node1TxStatus);
            node1Down = true;
            node1Status = UNAVAILABLE;
        } catch (Exception exception) {
            // transaction error during insertion, rollback and don't redo
            System.out.println("updateMovie - Error occurred during transaction in node 1...");
            node1TxManager.rollback(node1TxStatus);
            node1Status = ERROR;
        }

        // if node 1 transaction ready for commit or is unavailable
        if ((node1Status == OK || node1Status == UNAVAILABLE) && movie.getYear() < 1980) {
            node2TxStatus = node2TxManager.getTransaction(initTransactionDef());
            try {
                if (node2Down) {
                    throw new SQLException();
                }
                // try connection to node 2 before updating data
                node2Repo.tryConnection();
                System.out.println("updateMovie - Updating movie data to node 2...");
                node2Repo.updateMovie(movie);
                node2Status = OK;
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("updateMovie - Node 2 is currently down...");
                node2TxManager.rollback(node2TxStatus);
                node2Down = true;
                node2Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("updateMovie - Error occurred during transaction in node 2...");
                node2TxManager.rollback(node2TxStatus);
                node2Status = ERROR;
            }
        } else if ((node1Status == OK || node1Status == UNAVAILABLE) && movie.getYear() >= 1980) {
            node3TxStatus = node3TxManager.getTransaction(initTransactionDef());
            try {
                if (node3Down) {
                    throw new SQLException();
                }
                // try connection to node 3 before updating new data
                node3Repo.tryConnection();
                System.out.println("updateMovie - Updating movie data to node 3...");
                node3Repo.updateMovie(movie);
                node3Status = OK;
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("updateMovie - Node 3 is currently down...");
                node3TxManager.rollback(node3TxStatus);
                node3Down = true;
                node3Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("updateMovie - Error occurred during transaction in node 3...");
                node3TxManager.rollback(node3TxStatus);
                node3Status = ERROR;
            }
        } else {
            // else, an error has occurred and user has to try the query again in the UI
            System.out.println("updateMovie - Cannot perform data retrieval, throwing exception...");
            throw new TransactionErrorException();
        }

        // if node 2 or 3 failed, rollback node 1 transaction
        if (node2Status == ERROR || node3Status == ERROR) {
            node1TxManager.rollback(node1TxStatus);
            node1Status = ERROR;
            System.out.println("updateMovie - Cannot perform data retrieval, throwing exception...");
            throw new TransactionErrorException();
        }

        // if both nodes are ready for commit
        if (node1Status == OK || node1Status == UNAVAILABLE) {
            if (node2Status == OK) {
                // try node 2 commit transaction
                try {
                    // add transaction log
                    node2Repo.addLog(new Log(tUuid, "UPDATE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node2TxManager.commit(node2TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node2TxManager.rollback(node2TxStatus);
                    node2Status = ERROR;
                }
            } else if (node3Status == OK) {
                // try node 3 commit transaction
                try {
                    // add transaction log
                    node3Repo.addLog(new Log(tUuid, "UPDATE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node3TxManager.commit(node3TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node3TxManager.rollback(node3TxStatus);
                    node3Status = ERROR;
                }
            }
            // try node 1 commit transaction
            if (node1Status == OK) {
                try {
                    node1Repo.addLog(new Log(tUuid, "UPDATE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node1TxManager.commit(node1TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node1TxManager.rollback(node1TxStatus);
                    node1Status = ERROR;
                }
            }
            // recover if node 2 failed
            while (node2Status == ERROR) {
                node2TxStatus = node2TxManager.getTransaction(initTransactionDef());
                try {
                    // node 2 might be down, try connection
                    node2Repo.tryConnection();
                    node2Repo.updateMovie(movie);
                    node2Repo.addLog(new Log(tUuid, "UPDATE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node2TxManager.commit(node2TxStatus);
                    node2Status = OK; // node 2 now recovered
                } catch (SQLException sqlException) {
                    node2TxManager.rollback(node2TxStatus);
                    node2Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node2TxManager.rollback(node2TxStatus);
                }
            }
            // recover if node 3 failed
            while (node3Status == ERROR) {
                node3TxStatus = node3TxManager.getTransaction(initTransactionDef());
                try {
                    // node 2 might be down, try connection
                    node3Repo.tryConnection();
                    node3Repo.updateMovie(movie);
                    node3Repo.addLog(new Log(tUuid, "UPDATE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node3TxManager.commit(node3TxStatus);
                    node3Status = OK; // node 2 now recovered
                } catch (SQLException sqlException) {
                    node3TxManager.rollback(node3TxStatus);
                    node3Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node3TxManager.rollback(node3TxStatus);
                }
            }
            // recover if node 1 failed
            while (node1Status == ERROR) {
                node1TxStatus = node1TxManager.getTransaction(initTransactionDef());
                try {
                    // node 1 might be down, try connection
                    node1Repo.tryConnection();
                    node1Repo.updateMovie(movie);
                    node1Repo.addLog(new Log(tUuid, "UPDATE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node1TxManager.commit(node1TxStatus);
                    node1Status = OK; // node 1 now recovered
                } catch (SQLException sqlException) {
                    node1TxManager.rollback(node1TxStatus);
                    node1Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node1TxManager.rollback(node1TxStatus);
                }
            }
        }
        // if nodes suddenly went down or unavailable, signal server for db re-sync
        if (node1Status == UNAVAILABLE) {
            node1Down = true;
            resyncEnabled = true;
        }
        if (node2Status == UNAVAILABLE) {
            node2Down = true;
            resyncEnabled = true;
        }
        if (node3Status == UNAVAILABLE) {
            node3Down = true;
            resyncEnabled = true;
        }
        // operation cannot be done, throw exception as user will need to try it again
        if (node1Status == UNAVAILABLE && (node2Status == UNAVAILABLE || node3Status == UNAVAILABLE)) {
            System.out.println("updateMovie - Nodes are down, throwing exception...");
            throw new Exception ();
        }
    }

    public void deleteMovie(Movie movie) throws Exception {
        // STRATEGY: Delete existing movie data on node 1 first then deleting on node 2 or 3 depending on year of new movie
        // RECOVERY METHOD: Deferred Modification
        if (maintenance) {
            System.out.println("Server in maintenance...");
            throw new Exception ();
        }

        // disable db resync
        resyncEnabled = false;

        // node statuses
        int node1Status = INIT;
        int node2Status = INIT;
        int node3Status = INIT;
        // node transaction statuses (by order of commit / rollback, node2 / node3 then node1)
        TransactionStatus node1TxStatus = null;
        TransactionStatus node2TxStatus = null;
        TransactionStatus node3TxStatus = null;

        // generate new transaction uuid
        String tUuid = UUID.randomUUID().toString();

        node1TxStatus = node1TxManager.getTransaction(initTransactionDef());
        try {
            if (node1Down) {
                throw new SQLException();
            }
            // try connection to node 1 before deleting movie data
            node1Repo.tryConnection();
            System.out.println("deleteMovie - Deleting movie data to node 1...");
            node1Repo.deleteMovie(movie);
            node1Status = OK; // transaction is ready for commit
        } catch (SQLException sqlException) {
            // node 1 is currently down
            System.out.println("deleteMovie - Node 1 is currently down...");
            node1TxManager.rollback(node1TxStatus);
            node1Down = true;
            node1Status = UNAVAILABLE;
        } catch (Exception exception) {
            // transaction error during insertion, rollback and don't redo
            System.out.println("deleteMovie - Error occurred during transaction in node 1...");
            node1TxManager.rollback(node1TxStatus);
            node1Status = ERROR;
        }

        // if node 1 transaction ready for commit or is unavailable
        if ((node1Status == OK || node1Status == UNAVAILABLE) && movie.getYear() < 1980) {
            node2TxStatus = node2TxManager.getTransaction(initTransactionDef());
            try {
                if (node2Down) {
                    throw new SQLException();
                }
                // try connection to node 2 before deleting data
                node2Repo.tryConnection();
                System.out.println("deleteMovie - Deleting movie data to node 2...");
                node2Repo.deleteMovie(movie);
                node2Status = OK;
            } catch (SQLException sqlException) {
                // node 2 is currently down
                System.out.println("deleteMovie - Node 2 is currently down...");
                node2TxManager.rollback(node2TxStatus);
                node2Down = true;
                node2Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("deleteMovie - Error occurred during transaction in node 2...");
                node2TxManager.rollback(node2TxStatus);
                node2Status = ERROR;
            }
        } else if ((node1Status == OK || node1Status == UNAVAILABLE) && movie.getYear() >= 1980) {
            node3TxStatus = node3TxManager.getTransaction(initTransactionDef());
            try {
                if (node3Down) {
                    throw new SQLException();
                }
                // try connection to node 3 before deleting data
                node3Repo.tryConnection();
                System.out.println("deleteMovie - Deleting movie data to node 3...");
                node3Repo.deleteMovie(movie);
                node3Status = OK;
            } catch (SQLException sqlException) {
                // node 3 is currently down
                System.out.println("deleteMovie - Node 3 is currently down...");
                node3TxManager.rollback(node3TxStatus);
                node3Down = true;
                node3Status = UNAVAILABLE;
            } catch (Exception exception) {
                System.out.println("deleteMovie - Error occurred during transaction in node 3...");
                node3TxManager.rollback(node3TxStatus);
                node3Status = ERROR;
            }
        } else {
            // else, an error has occurred and user has to try the query again in the UI
            System.out.println("deleteMovie - Cannot perform data retrieval, throwing exception...");
            throw new TransactionErrorException();
        }

        // if node 2 or 3 failed, rollback node 1 transaction
        if (node2Status == ERROR || node3Status == ERROR) {
            node1TxManager.rollback(node1TxStatus);
            node1Status = ERROR;
            System.out.println("deleteMovie - Cannot perform data retrieval, throwing exception...");
            throw new TransactionErrorException();
        }

        // if both nodes are ready for commit
        if (node1Status == OK || node1Status == UNAVAILABLE) {
            if (node2Status == OK) {
                // try node 2 commit transaction
                try {
                    // add transaction log
                    node2Repo.addLog(new Log(tUuid, "DELETE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node2TxManager.commit(node2TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node2TxManager.rollback(node2TxStatus);
                    node2Status = ERROR;
                }
            } else if (node3Status == OK) {
                // try node 3 commit transaction
                try {
                    // add transaction log
                    node3Repo.addLog(new Log(tUuid, "DELETE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node3TxManager.commit(node3TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node3TxManager.rollback(node3TxStatus);
                    node3Status = ERROR;
                }
            }
            // try node 1 commit transaction
            if (node1Status == OK) {
                try {
                    node1Repo.addLog(new Log(tUuid, "DELETE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node1TxManager.commit(node1TxStatus);
                } catch (Exception e) {
                    // failure occurred during commit, rollback
                    node1TxManager.rollback(node1TxStatus);
                    node1Status = ERROR;
                }
            }
            // recover if node 2 failed
            while (node2Status == ERROR) {
                node2TxStatus = node2TxManager.getTransaction(initTransactionDef());
                try {
                    // node 2 might be down, try connection
                    node2Repo.tryConnection();
                    node2Repo.deleteMovie(movie);
                    node2Repo.addLog(new Log(tUuid, "DELETE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node2TxManager.commit(node2TxStatus);
                    node2Status = OK; // node 2 now recovered
                } catch (SQLException sqlException) {
                    node2TxManager.rollback(node2TxStatus);
                    node2Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node2TxManager.rollback(node2TxStatus);
                }
            }
            // recover if node 3 failed
            while (node3Status == ERROR) {
                node3TxStatus = node3TxManager.getTransaction(initTransactionDef());
                try {
                    // node 2 might be down, try connection
                    node3Repo.tryConnection();
                    node3Repo.deleteMovie(movie);
                    node3Repo.addLog(new Log(tUuid, "DELETE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node3TxManager.commit(node3TxStatus);
                    node3Status = OK; // node 2 now recovered
                } catch (SQLException sqlException) {
                    node3TxManager.rollback(node3TxStatus);
                    node3Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node3TxManager.rollback(node3TxStatus);
                }
            }
            // recover if node 1 failed
            while (node1Status == ERROR) {
                node1TxStatus = node1TxManager.getTransaction(initTransactionDef());
                try {
                    // node 1 might be down, try connection
                    node1Repo.tryConnection();
                    node1Repo.deleteMovie(movie);
                    node1Repo.addLog(new Log(tUuid, "DELETE", movie.getUuid(), movie.getYear(), getCurrTimestamp()));
                    node1TxManager.commit(node1TxStatus);
                    node1Status = OK; // node 1 now recovered
                } catch (SQLException sqlException) {
                    node1TxManager.rollback(node1TxStatus);
                    node1Status = UNAVAILABLE;
                } catch (Exception e) {
                    // another transaction failure, try again
                    node1TxManager.rollback(node1TxStatus);
                }
            }
        }
        // if nodes suddenly went down or unavailable, signal server for db re-sync
        if (node1Status == UNAVAILABLE) {
            node1Down = true;
            resyncEnabled = true;
        }
        if (node2Status == UNAVAILABLE) {
            node2Down = true;
            resyncEnabled = true;
        }
        if (node3Status == UNAVAILABLE) {
            node3Down = true;
            resyncEnabled = true;
        }
        // operation cannot be done, throw exception as user will need to try it again
        if (node1Status == UNAVAILABLE && (node2Status == UNAVAILABLE || node3Status == UNAVAILABLE)) {
            System.out.println("deleteMovie - Nodes are down, throwing exception...");
            throw new Exception ();
        }
    }

    // executes every 20 seconds if enabled, performs re-syncing of distributed database in case if system failures
    // occurred, specifically unavailability of nodes
    @Scheduled(initialDelay = 1000, fixedDelay = 20000)
    private void resyncDB() {
        // initial check (always done at start of server)
        if (initial) {
            try {
                // check if all nodes are online
                node1Repo.tryConnection();
                node2Repo.tryConnection();
                node3Repo.tryConnection();

                if (node1Repo.getNode2LogsCount() == node2Repo.getLogsCount() && node1Repo.getNode3LogsCount() == node3Repo.getLogsCount()) {
                    // disable re-sync as not needed
                    resyncEnabled = false;
                }
            } catch (Exception exception) {}
            initial = false;
        }

        // if there is a need for recovery
        if (resyncEnabled) {
            // set maintenance so that user queries will not be accepted during the process (similar to database being down)
            maintenance = true;

            System.out.println("resyncDB - Performing recovery...");

            boolean node1Recovery = true;
            boolean node2Recovery = true;
            boolean node3Recovery = true;
            boolean node1Recovered = false;
            boolean node2Recovered = false;
            boolean node3Recovered = false;

            // if node 1 was down during a transaction
            while (node1Recovery) {
                // check if all nodes are up in order to perform re-syncing
                try {
                    node1Repo.tryConnection();
                    node2Repo.tryConnection();
                    node3Repo.tryConnection();

                    // get recent node 1 log
                    Log recentNode1Log = node1Repo.getRecentLog();

                    // get logs that are more recent than node 1's recent log (transactions that occurred without node 1)
                    List<Log> node2Logs;
                    List<Log> node3Logs;
                    if (recentNode1Log == null) {
                        node2Logs = node2Repo.getAllLogs();
                        node3Logs = node3Repo.getAllLogs();
                    } else {
                        node2Logs = node2Repo.getLogs(recentNode1Log);
                        node3Logs = node3Repo.getLogs(recentNode1Log);
                    }

                    // update node 1 db with node 2 logs
                    for (int i = 0; i < node2Logs.size(); i++) {
                        if (recentNode1Log == null || !recentNode1Log.getUuid().equalsIgnoreCase(node2Logs.get(i).getUuid())) {
                            node1Recovered = true;
                            Movie movie = node2Repo.getMovieByUUID(node2Logs.get(i).getMovieUuid());
                            String op = node2Logs.get(i).getOp();
                            if (op.equalsIgnoreCase("INSERT")) {
                                node1Repo.addMovie(movie);
                            } else if (op.equalsIgnoreCase("UPDATE")) {
                                node1Repo.updateMovie(movie);
                            } else {
                                node1Repo.deleteMovie(movie);
                            }
                            node1Repo.addLog(node2Logs.get(i));
                        }
                    }

                    // update node 1 db with node 3 logs
                    for (int i = 0; i < node3Logs.size(); i++) {
                        if (recentNode1Log == null || !recentNode1Log.getUuid().equalsIgnoreCase(node3Logs.get(i).getUuid())) {
                            node1Recovered = true;
                            Movie movie = node3Repo.getMovieByUUID(node3Logs.get(i).getMovieUuid());
                            String op = node3Logs.get(i).getOp();
                            if (op.equalsIgnoreCase("INSERT")) {
                                node1Repo.addMovie(movie);
                            } else if (op.equalsIgnoreCase("UPDATE")) {
                                node1Repo.updateMovie(movie);
                            } else {
                                node1Repo.deleteMovie(movie);
                            }
                            node1Repo.addLog(node3Logs.get(i));
                        }
                    }

                    // node 1 recovered
                    System.out.println("resyncDB - Node 1 recovery finished...");
                    node1Down = false;
                    node1Recovery = false;
                } catch (SQLException sqlException) {
                    // at least one node is down, cannot perform re-sync
                    node1Recovery = false;
                } catch (Exception exception) {
                    // error occurred during a query, retry
                    exception.printStackTrace();
                }
            }

            // if node 2 was down during a transaction
            while (node2Recovery) {
                // check if both node 1 and node 2 is up to perform re-syncing
                try {
                    node1Repo.tryConnection();
                    node2Repo.tryConnection();

                    // get recent node 2 log
                    Log recentNode2Log = node2Repo.getRecentLog();

                    // get logs that are more recent than node 2's recent log (transactions that occurred without node 2)
                    List<Log> node1Logs;
                    if (recentNode2Log != null) {
                        node1Logs = node1Repo.getLogsForNode2(recentNode2Log);
                    } else {
                        node1Logs = node1Repo.getAllLogs();
                    }

                    // update node 2 db with node 1 logs
                    for (int i = 0; i < node1Logs.size(); i++) {
                        if (recentNode2Log == null || !recentNode2Log.getUuid().equalsIgnoreCase(node1Logs.get(i).getUuid())) {
                            node2Recovered = true;
                            Movie movie = node1Repo.getMovieByUUID(node1Logs.get(i).getMovieUuid());
                            String op = node1Logs.get(i).getOp();
                            if (op.equalsIgnoreCase("INSERT")) {
                                node2Repo.addMovie(movie);
                            } else if (op.equalsIgnoreCase("UPDATE")) {
                                node2Repo.updateMovie(movie);
                            } else {
                                node2Repo.deleteMovie(movie);
                            }
                            node2Repo.addLog(node1Logs.get(i));
                        }
                    }

                    // node 2 recovered
                    System.out.println("resyncDB - Node 2 recovery finished...");
                    node2Down = false;
                    node2Recovery = false;
                } catch (SQLException sqlException) {
                    node2Recovery = false;
                } catch (Exception exception) {}
            }

            // if node 3 was down during a transaction
            while (node3Recovery) {
                // check if both node 1 and node 3 is up to perform re-syncing
                try {
                    node1Repo.tryConnection();
                    node3Repo.tryConnection();

                    // get recent node 3 log
                    Log recentNode3Log = node3Repo.getRecentLog();

                    // get logs that are more recent than node 3's recent log (transactions that occurred without node 3)
                    List<Log> node1Logs;
                    if (recentNode3Log != null) {
                        node1Logs = node1Repo.getLogsForNode3(recentNode3Log);
                    } else {
                        node1Logs = node1Repo.getAllLogs();
                    }

                    // update node 3 db with node 1 logs
                    for (int i = 0; i < node1Logs.size(); i++) {
                        if (recentNode3Log == null || !recentNode3Log.getUuid().equalsIgnoreCase(node1Logs.get(i).getUuid())) {
                            node3Recovered = true;
                            Movie movie = node1Repo.getMovieByUUID(node1Logs.get(i).getMovieUuid());
                            String op = node1Logs.get(i).getOp();
                            if (op.equalsIgnoreCase("INSERT")) {
                                node3Repo.addMovie(movie);
                            } else if (op.equalsIgnoreCase("UPDATE")) {
                                node3Repo.updateMovie(movie);
                            } else {
                                node3Repo.deleteMovie(movie);
                            }
                            node3Repo.addLog(node1Logs.get(i));
                        }
                    }

                    // node 3 recovered
                    System.out.println("resyncDB - Node 3 recovery finished...");
                    node3Down = false;
                    node3Recovery = false;
                } catch (SQLException sqlException) {
                    node3Recovery = false;
                } catch (Exception exception) {}
            }

            // check if all nodes are in OK state and do not need recovery anymore
            try {
                // all nodes must be available
                node1Repo.tryConnection();
                node2Repo.tryConnection();
                node3Repo.tryConnection();

                if (node1Repo.getNode2LogsCount() == node2Repo.getLogsCount() && node1Repo.getNode3LogsCount() == node3Repo.getLogsCount()) {
                    node1Recovered = true;
                    node2Recovered = true;
                    node3Recovered = true;
                }
            } catch (Exception exception) {}

            // if all nodes have recovered successfully, disable re-sync and delete logs from each node to have more space
            while (node1Recovered && node2Recovered && node3Recovered && maintenance) {
                // try deleting logs on each node
                try {
                    // try connection first to each node
                    node1Repo.tryConnection();
                    node2Repo.tryConnection();
                    node3Repo.tryConnection();

                    // delete logs on each node
                    node1Repo.deleteLogs();
                    node2Repo.deleteLogs();
                    node3Repo.deleteLogs();

                    // logs deleted successfully, release maintenance and disable re-sync
                    maintenance = false;
                    resyncEnabled = false;

                    System.out.println("resyncDB - All logs deleted from each node...");
                } catch (SQLException sqlException) {
                    // at least one node is down, cannot perform deletion of logs
                    maintenance = false;
                } catch (Exception exception) {
                    // error occurred during query, repeat process
                }
            }

            // if at least one node is up, release maintenance and disable re-sync for now
            if (!node1Down || !node2Down || !node3Down) {
                maintenance = false;
                resyncEnabled = false;
            }
        }
    }

}
