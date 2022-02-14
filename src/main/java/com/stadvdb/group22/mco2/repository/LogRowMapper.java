package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.model.Log;

import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LogRowMapper implements RowMapper<Log> {

    @Override
    public Log mapRow(ResultSet rs, int rowNum) throws SQLException {
        Log log = new Log();
        log.setUuid(rs.getString("t_uuid"));
        log.setOp(rs.getString("t_op"));
        log.setMovieUuid(rs.getString("movie_uuid"));
        log.setMovieYear(rs.getInt("movie_yr"));
        log.setTs(rs.getTimestamp("ts"));
        return log;
    }

}
