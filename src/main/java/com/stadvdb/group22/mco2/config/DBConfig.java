package com.stadvdb.group22.mco2.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DBConfig {

    @Bean(name="node1")
    @ConfigurationProperties(prefix="spring.node1")
    public DataSource node1() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="node1Jdbc")
    public JdbcTemplate node1Jdbc(@Qualifier("node1") DataSource datasource) {
        return new JdbcTemplate(datasource);
    }

    @Bean(name="node2")
    @ConfigurationProperties(prefix="spring.node2")
    public DataSource node2() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="node2Jdbc")
    public JdbcTemplate node2Jdbc(@Qualifier("node2") DataSource datasource) {
        return new JdbcTemplate(datasource);
    }

    @Bean(name="node3")
    @ConfigurationProperties(prefix="spring.node3")
    public DataSource node3() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="node3Jdbc")
    public JdbcTemplate node3Jdbc(@Qualifier("node3") DataSource datasource) {
        return new JdbcTemplate(datasource);
    }

}
