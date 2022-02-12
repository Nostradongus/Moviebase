package com.stadvdb.group22.mco2.config;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

@Configuration
@EnableScheduling // for distributed db re-syncing (resync() in DistributedDBService class)
public class DBConfig {

    // CHANGE DATABASE CONNECTION LOGIN TIMEOUT VALUE HERE
    public static final int LOGIN_TIME_OUT = 5;

    // CHANGE TRANSACTION ISOLATION LEVEL HERE
    public static final int ISOLATION_LEVEL = TransactionDefinition.ISOLATION_READ_UNCOMMITTED;

    // CHANGE TRANSACTION TIMEOUT VALUE HERE
    public static final int T_TIME_OUT = 30;


    // NODE 1 (CENTRAL NODE) CONNECTION DETAILS
    public static final String node1Url = "jdbc:mysql://stadvdb-mco2-group22-1.mysql.database.azure.com:3306/movies_all";
    public static final String node1Username = "jadie";
    public static final String node1Password = "Jekyll150!";

    // NODE 2 CONNECTION DETAILS
    public static final String node2Url = "jdbc:mysql://stadvdb-mco2-group22-2.mysql.database.azure.com:3306/movies_before_1980";
    public static final String node2Username = "ponce";
    public static final String node2Password = "P@ssw0rd123";

    // NODE 3 CONNECTION DETAILS
    public static final String node3Url = "jdbc:mysql://stadvdb-mco2-group22-3.mysql.database.azure.com:3306/movies_after_1980";
    public static final String node3Username = "lopez";
    public static final String node3Password = "password12!";

    // CONNECTION DRIVER CLASS
    public static final String driverClassName = "com.mysql.cj.jdbc.Driver";

    /***** NODE 1 / CENTRAL NODE CONFIGURATIONS *****/
    @Bean(name="node1")
    public DataSource node1() {
        // setup datasource
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(driverClassName);
        dataSourceBuilder.url(node1Url);
        dataSourceBuilder.username(node1Username);
        dataSourceBuilder.password(node1Password);
        DataSource datasource = dataSourceBuilder.build();
        return datasource;
    }

    @Bean(name="node1TxManager")
    public DataSourceTransactionManager node1TxManager(@Qualifier("node1") DataSource datasource) {
        return new DataSourceTransactionManager(datasource);
    }

    @Bean(name="node1Jdbc")
    public JdbcTemplate node1Jdbc(@Qualifier("node1") DataSource datasource) {
        return new JdbcTemplate(datasource);
    }

    @Bean(name="node1TxTemplate")
    public TransactionTemplate node1TxTemplate(@Qualifier("node1TxManager") DataSourceTransactionManager txManager) {
        TransactionTemplate node1TxTemplate = new TransactionTemplate(txManager);
        node1TxTemplate.setIsolationLevel(ISOLATION_LEVEL);
        node1TxTemplate.setTimeout(T_TIME_OUT);
        return node1TxTemplate;
    }

    /***** NODE 2 CONFIGURATIONS *****/
    @Bean(name="node2")
    public DataSource node2() {
        // setup datasource
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(driverClassName);
        dataSourceBuilder.url(node2Url);
        dataSourceBuilder.username(node2Username);
        dataSourceBuilder.password(node2Password);
        DataSource datasource = dataSourceBuilder.build();
        return datasource;
    }

    @Bean(name="node2Jdbc")
    public JdbcTemplate node2Jdbc(@Qualifier("node2") DataSource datasource) {
        return new JdbcTemplate(datasource);
    }

    @Bean(name="node2TxManager")
    public DataSourceTransactionManager node2TxManager(@Qualifier("node2") DataSource datasource) {
        return new DataSourceTransactionManager(datasource);
    }

    @Bean(name="node2TxTemplate")
    public TransactionTemplate node2TxTemplate(@Qualifier("node2TxManager") DataSourceTransactionManager txManager) {
        TransactionTemplate node2TxTemplate = new TransactionTemplate(txManager);
        node2TxTemplate.setIsolationLevel(ISOLATION_LEVEL);
        node2TxTemplate.setTimeout(T_TIME_OUT);
        return node2TxTemplate;
    }

    /***** NODE 3 CONFIGURATIONS *****/
    @Bean(name="node3")
    public DataSource node3() {
        // setup datasource
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(driverClassName);
        dataSourceBuilder.url(node3Url);
        dataSourceBuilder.username(node3Username);
        dataSourceBuilder.password(node3Password);
        DataSource datasource = dataSourceBuilder.build();
        return datasource;
    }

    @Bean(name="node3Jdbc")
    public JdbcTemplate node3Jdbc(@Qualifier("node3") DataSource datasource) {
        return new JdbcTemplate(datasource);
    }

    @Bean(name="node3TxManager")
    public DataSourceTransactionManager node3TxManager(@Qualifier("node3") DataSource datasource) {
        return new DataSourceTransactionManager(datasource);
    }

    @Bean(name="node3TxTemplate")
    public TransactionTemplate node3TxTemplate(@Qualifier("node3TxManager") DataSourceTransactionManager txManager) {
        TransactionTemplate node3TxTemplate = new TransactionTemplate(txManager);
        node3TxTemplate.setIsolationLevel(ISOLATION_LEVEL);
        node3TxTemplate.setTimeout(T_TIME_OUT);
        return node3TxTemplate;
    }

    @Bean
    public ReentrantLock lock() {
        ReentrantLock lock = new ReentrantLock(true);
        return lock;
    }

}
