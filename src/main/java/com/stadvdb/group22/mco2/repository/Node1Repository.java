package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import com.stadvdb.group22.mco2.repository.MovieRowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.List;

@Repository
public class Node1Repository {

    // JDBC data source connection to central node / node 1
    @Autowired
    @Qualifier("node1Jdbc")
    private JdbcTemplate node1;

    // For transaction management
    @Autowired
    @Qualifier("node1TxTemplate")
    private TransactionTemplate txTemplate;

    // TODO: Application Features
    // - [X] Show movies by pages
    // - [X] Search for specific movie/s
    // - [X] Add a new movie record
    // - [X] Update an existing movie record
    // - [X] Delete a movie record
    // - [X] Display how many movies were produced in each genre
    // - [X] Display how many movies were produced by each director
    // - [X] Display how many movies each actor starred in
    // - [X] Display how many movies were produced in each year

    public Page<Movie> getMoviesByPage(Pageable pageable) throws TransactionException {
        // query will be reading data only
        txTemplate.setReadOnly(true);
        // throw TransactionException to be handled by controller
        return txTemplate.execute(status -> {
            int total = node1.queryForObject("SELECT COUNT(*) FROM movies", Integer.class);
            String sqlQuery = "SELECT * FROM movies LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Movie> movies = node1.query(sqlQuery, new MovieRowMapper());
            return new PageImpl<>(movies, pageable, total);
        });
    }

    public Page<Movie> searchMoviesByPage(String year, String title, String genre,
                                          String actor, String director, Pageable pageable) throws TransactionException {
        // query will be reading data only
        txTemplate.setReadOnly(true);
        // execute transaction
        return txTemplate.execute(status -> {
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.
                append(" FROM movies WHERE ").
                append("yr = COALESCE(" + year + ", yr) AND ").
                append("title = COALESCE(" + title + ", title) AND ").
                append("genre = COALESCE(" + genre + ", genre) AND ").
                append("((actor1 = COALESCE(" + actor +  ", actor1)) OR (actor2 = COALESCE(" + actor + ", actor2))) AND ").
                append("director = COALESCE(" + director + ", director)");
            int total = node1.queryForObject("SELECT COUNT(*)" + sqlQuery.toString(), Integer.class);
            List<Movie> movies = node1.query(
                    "SELECT *" + sqlQuery.toString() + " LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset(),
                    new MovieRowMapper()
            );
            return new PageImpl<>(movies, pageable, total);
        });
    }

    public Page<Report> getMoviesPerGenreByPage(Pageable pageable) throws TransactionException {
        // query will be reading data only
        txTemplate.setReadOnly(true);
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node1.queryForObject("SELECT COUNT(DISTINCT genre) FROM movies", Integer.class);
            String sqlQuery = "SELECT genre AS label, COUNT(*) AS count FROM movies GROUP BY genre LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node1.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public Page<Report> getMoviesPerDirectorByPage(Pageable pageable) throws TransactionException {
        // query will be reading data only
        txTemplate.setReadOnly(true);
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node1.queryForObject("SELECT COUNT(DISTINCT director) FROM movies", Integer.class);
            String sqlQuery = "SELECT director AS label, COUNT(*) AS count FROM movies GROUP BY director LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node1.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public Page<Report> getMoviesPerActorByPage(Pageable pageable) throws TransactionException {
        // query will be reading data only
        txTemplate.setReadOnly(true);
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node1.queryForObject("SELECT COUNT(DISTINCT actor1) FROM movies", Integer.class);
            String sqlQuery = "SELECT actor1 AS label, COUNT(*) AS count FROM movies GROUP BY actor1 LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node1.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public Page<Report> getMoviesPerYearByPage(Pageable pageable) throws TransactionException {
        // query will be reading data only
        txTemplate.setReadOnly(true);
        // execute transaction
        return txTemplate.execute(status -> {
            int total = node1.queryForObject("SELECT COUNT(DISTINCT yr) FROM movies", Integer.class);
            String sqlQuery = "SELECT yr AS label, COUNT(*) AS count FROM movies GROUP BY yr LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
            List<Report> reports = node1.query(sqlQuery, new ReportRowMapper());
            return new PageImpl<>(reports, pageable, total);
        });
    }

    public void addMovie(Movie movie) throws TransactionException {
        // query will involve writing operation
        txTemplate.setReadOnly(false);
        // execute transaction
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    String sqlQuery = "INSERT INTO movies (title, yr, genre, actor1, actor2, director) VALUES (?, ?, ?, ?, ?, ?)";
                    node1.update(
                            sqlQuery,
                            movie.getTitle(), movie.getYear(), movie.getGenre(),
                            movie.getActor1(), movie.getActor2(), movie.getDirector()
                    );
                } catch (Exception e) {
                    // rollback if failure occurs
                    status.setRollbackOnly();
                }
            }
        });
    }

    public void updateMovie(Movie movie) throws TransactionException {
        // query will involve writing operation
        txTemplate.setReadOnly(false);
        // execute transaction
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    String sqlQuery = "UPDATE movies SET title=?, yr=?, genre=?, actor1=?, actor2=?, director=? WHERE id=?";
                    node1.update(
                            sqlQuery,
                            movie.getTitle(), movie.getYear(), movie.getGenre(),
                            movie.getActor1(), movie.getActor2(), movie.getDirector(),
                            movie.getId()
                    );
                } catch (Exception e) {
                    // rollback if failure occurs
                    status.setRollbackOnly();
                }
            }
        });
    }

    public void deleteMovie(Movie movie) throws TransactionException {
        // query will involve writing operation
        txTemplate.setReadOnly(false);
        // execute transaction
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    String sqlQuery = "DELETE FROM movies WHERE id=?";
                    node1.update(sqlQuery, movie.getId());
                } catch (Exception e) {
                    // rollback if failure occurs
                    status.setRollbackOnly();
                }
            }
        });
    }

}
