package com.crystal.jobs.utils;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.util.Objects;
import java.util.Scanner;

public class JdbcConnector {

    private static JdbcConnector INSTANCE;
    private  final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    private  final String DB_URL = "jdbc:mysql://localhost:3306/conference";
    private  final String DB_USER_NAME = "root";
    private  final String DB_PASSWORD = "Shanti2022!";

    public String getDRIVER_CLASS_NAME() {
        return DRIVER_CLASS_NAME;
    }

    public String getDB_URL() {
        return DB_URL;
    }

    public String getDB_USER_NAME() {
        return DB_USER_NAME;
    }

    public String getDB_PASSWORD() {
        return DB_PASSWORD;
    }

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
