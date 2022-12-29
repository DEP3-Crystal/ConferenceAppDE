package com.crystal.jobs.utils;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.util.Objects;
import java.util.Scanner;

public class JdbcConnector {

    private static JdbcConnector INSTANCE;

    private JdbcConnector() {
    }

    public static synchronized JdbcConnector getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new JdbcConnector();
        }
        return INSTANCE;
    }
    public <T> JdbcIO.Read<T> databaseInit(String path) {
        String text = new Scanner(Objects.requireNonNull(JdbcConnector.class.getResourceAsStream(path)),  "UTF-8").useDelimiter("\\A").next();
            return JdbcIO.<T>read()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/conference")
                            .withUsername("root")
                            .withPassword("Shanti2022!"))
                    .withQuery(text);

    }

    public <T> JdbcIO.Write<T> databaseWrite(String path) {
        String text = new Scanner(Objects.requireNonNull(JdbcConnector.class.getResourceAsStream(path)),  "UTF-8").useDelimiter("\\A").next();
            return JdbcIO.<T>write()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/conference")
                            .withUsername("root")
                            .withPassword("Shanti2022!"))
                    .withStatement(text);
        }


}
