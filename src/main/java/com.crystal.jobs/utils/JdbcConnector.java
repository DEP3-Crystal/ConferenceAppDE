package com.crystal.jobs.utils;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public <T> JdbcIO.Read<T> databaseInit(String path) throws IOException {
        try(Stream<String> lines = Files.lines(Path.of(path))) {
            return JdbcIO.<T>read()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/conference")
                            .withUsername("root")
                            .withPassword("Shanti2022!"))
                    .withQuery(lines.collect(Collectors.joining()));
        }
    }

    public <T> JdbcIO.Write<T> databaseWrite(String path) throws IOException {
        try(Stream<String> lines = Files.lines(Path.of(path))) {
            return JdbcIO.<T>write()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/conference")
                            .withUsername("root")
                            .withPassword("Shanti2022!"))
                    .withStatement(lines.collect(Collectors.joining()));
        }
    }

}
