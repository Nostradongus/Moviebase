package com.stadvdb.group22.mco2.repository;

import com.stadvdb.group22.mco2.model.Movie;
import com.stadvdb.group22.mco2.model.Report;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ReportRowMapper implements RowMapper<Report> {

    @Override
    public Report mapRow(ResultSet rs, int rowNum) throws SQLException {
        Report report = new Report();
        report.setLabel(rs.getString("label"));
        report.setCount(rs.getInt("count"));
        return report;
    }

}
