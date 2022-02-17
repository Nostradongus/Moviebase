package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.config.DBConfig;
import com.stadvdb.group22.mco2.model.Log;
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

import java.sql.*;
import java.util.List;

@Repository
public class Node3Repository {

    // JDBC data source connection to node 2
    @Autowired
    @Qualifier("node3Jdbc")
    private JdbcTemplate node3;

    public void tryConnection() throws SQLException {
        // try connection to database, if database is down then throw SQLException
        DriverManager.setLoginTimeout(DBConfig.LOGIN_TIME_OUT);

        // TODO: [GLOBAL FAILURE RECOVERY CASE #4 - NODE 3 IS DOWN]
        // intentionally set wrong password
        Connection connection = DriverManager.getConnection(DBConfig.node3Url, DBConfig.node3Username, DBConfig.node3Password);

        // close connection afterwards if successful
        connection.close();
    }

    public Movie getMovieByUUID(String uuid) throws DataAccessException {
        String sqlQuery = "SELECT * FROM movies WHERE uuid=?";
        List<Movie> movies = node3.query (sqlQuery, new MovieRowMapper(), uuid);
        return movies.size () > 0 ? movies.get(0) : null;
    }

    public Page<Movie> getMoviesByPage(Pageable pageable) throws DataAccessException {
        int total = node3.queryForObject("SELECT COUNT(*) FROM movies", Integer.class);
        String sqlQuery = "SELECT * FROM movies LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
        List<Movie> movies = node3.query(sqlQuery, new MovieRowMapper());
        return new PageImpl<>(movies, pageable, total);
    }

    public List<Movie> getMovies() throws DataAccessException {
        return node3.query("SELECT * FROM movies", new MovieRowMapper());
    }

    public int getNumOfMovies() throws DataAccessException {
        return node3.queryForObject("SELECT COUNT(*) FROM movies", Integer.class);
    }

    public Page<Movie> searchMoviesByPage(Movie movie, Pageable pageable) throws DataAccessException {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.
                append(" FROM movies WHERE ").
                append("yr = COALESCE(" + movie.getYear() + ", yr) AND ").
                append("title = COALESCE(" + movie.getTitle() + ", title) AND ").
                append("genre = COALESCE(" + movie.getGenre() + ", genre) AND ").
                append("((actor1 = COALESCE(" + movie.getActor1() +  ", actor1)) OR (actor2 = COALESCE(" + movie.getActor1() + ", actor2))) AND ").
                append("director = COALESCE(" + movie.getDirector() + ", director)");
        int total = node3.queryForObject("SELECT COUNT(*)" + sqlQuery.toString(), Integer.class);
        List<Movie> movies = node3.query(
                "SELECT *" + sqlQuery.toString() + " LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset(),
                new MovieRowMapper()
        );
        return new PageImpl<>(movies, pageable, total);
    }

    public Page<Report> getMoviesPerGenreByPage(Pageable pageable) throws DataAccessException {
        int total = node3.queryForObject("SELECT COUNT(DISTINCT genre) FROM movies", Integer.class);
        String sqlQuery = "SELECT genre AS label, COUNT(*) AS count FROM movies GROUP BY genre LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
        List<Report> reports = node3.query(sqlQuery, new ReportRowMapper());
        return new PageImpl<>(reports, pageable, total);
    }

    public List<Report> getMoviesPerGenre() throws DataAccessException {
        return node3.query("SELECT genre AS label, COUNT(*) AS count FROM movies GROUP BY genre", new ReportRowMapper());
    }

    public int getNumOfGenres() throws DataAccessException {
        return node3.queryForObject("SELECT COUNT(DISTINCT genre) FROM movies", Integer.class);
    }

    public Page<Report> getMoviesPerDirectorByPage(Pageable pageable) throws DataAccessException {
        int total = node3.queryForObject("SELECT COUNT(DISTINCT director) FROM movies", Integer.class);
        String sqlQuery = "SELECT director AS label, COUNT(*) AS count FROM movies GROUP BY director LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
        List<Report> reports = node3.query(sqlQuery, new ReportRowMapper());
        return new PageImpl<>(reports, pageable, total);
    }

    public List<Report> getMoviesPerDirector() throws DataAccessException {
        return node3.query("SELECT director AS label, COUNT(*) AS count FROM movies GROUP BY director", new ReportRowMapper());
    }

    public int getNumOfDirectors() throws DataAccessException {
        return node3.queryForObject("SELECT COUNT(DISTINCT director) FROM movies", Integer.class);
    }

    public Page<Report> getMoviesPerActorByPage(Pageable pageable) throws DataAccessException {
        int total = node3.queryForObject("SELECT COUNT(DISTINCT actor1) FROM movies", Integer.class);
        String sqlQuery = "SELECT actor1 AS label, COUNT(*) AS count FROM movies GROUP BY actor1 LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
        List<Report> reports = node3.query(sqlQuery, new ReportRowMapper());
        return new PageImpl<>(reports, pageable, total);
    }

    public List<Report> getMoviesPerActor() throws DataAccessException {
        return node3.query("SELECT actor1 AS label, COUNT(*) AS count FROM movies GROUP BY actor1", new ReportRowMapper());
    }

    public int getNumOfActors() throws DataAccessException {
        return node3.queryForObject("SELECT COUNT(DISTINCT actor1) FROM movies", Integer.class);
    }

    public Page<Report> getMoviesPerYearByPage(Pageable pageable) throws DataAccessException {
        int total = node3.queryForObject("SELECT COUNT(DISTINCT yr) FROM movies", Integer.class);
        String sqlQuery = "SELECT yr AS label, COUNT(*) AS count FROM movies GROUP BY yr LIMIT " + pageable.getPageSize() + " OFFSET " + pageable.getOffset();
        List<Report> reports = node3.query(sqlQuery, new ReportRowMapper());
        return new PageImpl<>(reports, pageable, total);
    }

    public List<Report> getMoviesPerYear() throws DataAccessException {
        return node3.query("SELECT yr AS label, COUNT(*) AS count FROM movies GROUP BY yr", new ReportRowMapper());
    }

    public int getNumOfYears() throws DataAccessException {
        return node3.queryForObject("SELECT COUNT(DISTINCT yr) FROM movies", Integer.class);
    }

    public void addMovie(Movie movie) throws DataAccessException {
        String sqlQuery = "INSERT INTO movies (title, yr, genre, actor1, actor2, director, uuid) VALUES (?, ?, ?, ?, ?, ?, ?)";
        node3.execute(sqlQuery, new PreparedStatementCallback<Boolean>() {
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
    }

    public void updateMovie(Movie movie) throws DataAccessException {
        String sqlQuery = "UPDATE movies SET title=?, yr=?, genre=?, actor1=?, actor2=?, director=? WHERE uuid=?";
        node3.execute(sqlQuery, new PreparedStatementCallback<Boolean>() {
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
    }

    public void deleteMovie(Movie movie) throws DataAccessException {
        String sqlQuery = "DELETE FROM movies WHERE uuid=?";
        node3.update(sqlQuery, movie.getUuid());
    }

    public void addLog(Log log) {
        String sqlQuery = "INSERT INTO t_log(t_uuid,t_op,movie_uuid,movie_yr,ts) VALUES (?,?,?,?,?)";
        node3.update(sqlQuery, log.getUuid(), log.getOp(), log.getMovieUuid(), log.getMovieYear(), log.getTs());
    }

    public Log getRecentLog() {
        List<Log> logs = node3.query("SELECT * FROM t_log ORDER BY ts DESC LIMIT 1", new LogRowMapper());
        return logs.size() > 0 ? logs.get(0) : null;
    }

    public List<Log> getAllLogs() {
        return node3.query("SELECT * FROM t_log", new LogRowMapper());
    }

    public List<Log> getLogs(Log log) {
        return node3.query("SELECT * FROM t_log WHERE ts > ?", new LogRowMapper(), log.getTs());
    }

    public int getLogsCount() {
        Integer count = node3.queryForObject("SELECT COUNT(*) FROM t_log", Integer.class);
        return count == null ? 0 : count;
    }

    public void deleteLogs() {
        node3.execute("DELETE FROM t_log");
    }
}
