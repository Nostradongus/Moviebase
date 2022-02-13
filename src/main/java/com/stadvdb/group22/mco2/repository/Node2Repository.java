package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.config.DBConfig;
import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.*;
import java.util.List;

@Repository
public class Node2Repository {
    // transaction manager for node 2, needed to set isolation level
    @Autowired
    @Qualifier("node2TxTemplate")
    private TransactionTemplate txTemplate;

    // JDBC data source connection to node 2
    @Autowired
    @Qualifier("node2Jdbc")
    private JdbcTemplate node2;

    public void tryConnection() throws SQLException {
        // try connection to database, if database is down then throw SQLException
        DriverManager.setLoginTimeout(5);
        Connection connection = DriverManager.getConnection(DBConfig.node2Url, DBConfig.node2Username, DBConfig.node1Password);

        // close connection afterwards if successful
        connection.close();
    }

    public Movie getMovieByUUID(String uuid) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            String sqlQuery = "SELECT * FROM movies WHERE uuid=?";
            List<Movie> movies = node2.query (sqlQuery, new MovieRowMapper(), uuid);
            return movies.size () > 0 ? movies.get(0) : null;
        });
    }

    public Page<Movie> getMoviesByPage(Pageable pageable) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node2.queryForObject("SELECT COUNT(*) FROM movies", Integer.class);
            String sqlQuery = "SELECT * FROM movies LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Movie> movies = node2.query(sqlQuery, new MovieRowMapper());
            return new PageImpl<>(movies, pageable, total);
        });
    }

    public List<Movie> getMovies() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.query("SELECT * FROM movies", new MovieRowMapper()));
    }

    public int getNumOfMovies() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.queryForObject("SELECT COUNT(*) FROM movies", Integer.class));
    }

    public Page<Movie> searchMoviesByPage(Movie movie, Pageable pageable) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.
                    append(" FROM movies WHERE ").
                    append("yr = COALESCE(" + movie.getYear() + ", yr) AND ").
                    append("title = COALESCE(" + movie.getTitle() + ", title) AND ").
                    append("genre = COALESCE(" + movie.getGenre() + ", genre) AND ").
                    append("((actor1 = COALESCE(" + movie.getActor1() +  ", actor1)) OR (actor2 = COALESCE(" + movie.getActor1() + ", actor2))) AND ").
                    append("director = COALESCE(" + movie.getDirector() + ", director)");
            int total = node2.queryForObject("SELECT COUNT(*)" + sqlQuery.toString(), Integer.class);
            List<Movie> movies = node2.query(
                    "SELECT *" + sqlQuery.toString() + " LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset(),
                    new MovieRowMapper()
            );
            return new PageImpl<>(movies, pageable, total);
        });
    }

    public Page<Report> getMoviesPerGenreByPage(Pageable pageable) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node2.queryForObject("SELECT COUNT(DISTINCT genre) FROM movies", Integer.class);
            String sqlQuery = "SELECT genre AS label, COUNT(*) AS count FROM movies GROUP BY genre LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node2.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public List<Report> getMoviesPerGenre() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.query("SELECT genre AS label, COUNT(*) AS count FROM movies GROUP BY genre", new ReportRowMapper()));
    }

    public int getNumOfGenres() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.queryForObject("SELECT COUNT(DISTINCT genre) FROM movies", Integer.class));
    }

    public Page<Report> getMoviesPerDirectorByPage(Pageable pageable) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node2.queryForObject("SELECT COUNT(DISTINCT director) FROM movies", Integer.class);
            String sqlQuery = "SELECT director AS label, COUNT(*) AS count FROM movies GROUP BY director LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node2.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public List<Report> getMoviesPerDirector() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.query("SELECT director AS label, COUNT(*) AS count FROM movies GROUP BY director", new ReportRowMapper()));
    }

    public int getNumOfDirectors() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.queryForObject("SELECT COUNT(DISTINCT director) FROM movies", Integer.class));
    }

    public Page<Report> getMoviesPerActorByPage(Pageable pageable) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node2.queryForObject("SELECT COUNT(DISTINCT actor1) FROM movies", Integer.class);
            String sqlQuery = "SELECT actor1 AS label, COUNT(*) AS count FROM movies GROUP BY actor1 LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node2.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public List<Report> getMoviesPerActor() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.query("SELECT actor1 AS label, COUNT(*) AS count FROM movies GROUP BY actor1", new ReportRowMapper()));
    }

    public int getNumOfActors() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.queryForObject("SELECT COUNT(DISTINCT actor1) FROM movies", Integer.class));
    }

    public Page<Report> getMoviesPerYearByPage(Pageable pageable) throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node2.queryForObject("SELECT COUNT(DISTINCT yr) FROM movies", Integer.class);
            String sqlQuery = "SELECT yr AS label, COUNT(*) AS count FROM movies GROUP BY yr LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node2.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public List<Report> getMoviesPerYear() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.query("SELECT yr AS label, COUNT(*) AS count FROM movies GROUP BY yr", new ReportRowMapper()));
    }

    public int getNumOfYears() throws TransactionException {
        // execute transaction
        return txTemplate.execute(status -> node2.queryForObject("SELECT COUNT(DISTINCT yr) FROM movies", Integer.class));
    }

    public void addMovie(Movie movie) throws TransactionException {
        // execute transaction
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    String sqlQuery = "INSERT INTO movies (title, yr, genre, actor1, actor2, director, uuid) VALUES (?, ?, ?, ?, ?, ?, ?)";
                    node2.execute(sqlQuery, new PreparedStatementCallback<Boolean>() {
                        @Override
                        public Boolean doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                            ps.setString(1, movie.getTitle());
                            ps.setInt(2, movie.getYear());
                            ps.setString(3, movie.getGenre());
                            ps.setString(4, movie.getActor1());
                            if (movie.getActor2().equalsIgnoreCase("")) {
                                ps.setNull(5, Types.VARCHAR);
                            } else {
                                ps.setString(5, movie.getActor2());
                            }
                            ps.setString(6, movie.getDirector());
                            ps.setString(7, movie.getUuid());
                            return ps.execute();
                        }
                    });
                } catch (Exception e) {
                    // rollback due to failure
                    status.setRollbackOnly();
                    throw e;
                }
            }
        });
    }

    public void updateMovie(Movie movie) throws TransactionException {
        // execute transaction
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    String sqlQuery = "UPDATE movies SET title=?, yr=?, genre=?, actor1=?, actor2=?, director=? WHERE uuid=?";
                    node2.execute(sqlQuery, new PreparedStatementCallback<Boolean>() {
                        @Override
                        public Boolean doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                            ps.setString(1, movie.getTitle());
                            ps.setInt(2, movie.getYear());
                            ps.setString(3, movie.getGenre());
                            ps.setString(4, movie.getActor1());
                            if (movie.getActor2().equalsIgnoreCase("")) {
                                ps.setNull(5, Types.VARCHAR);
                            } else {
                                ps.setString(5, movie.getActor2());
                            }
                            ps.setString(6, movie.getDirector());
                            ps.setString(7, movie.getUuid());
                            return ps.execute();
                        }
                    });
                } catch (Exception e) {
                    // rollback due to failure
                    status.setRollbackOnly();
                    throw e;
                }
            }
        });
    }

    public void deleteMovie(Movie movie) throws TransactionException {
        // execute transaction
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    String sqlQuery = "DELETE FROM movies WHERE uuid=?";
                    node2.update(sqlQuery, movie.getUuid());
                } catch (Exception e) {
                    // rollback due to failure
                    status.setRollbackOnly();
                    throw e;
                }
            }
        });
    }
}
