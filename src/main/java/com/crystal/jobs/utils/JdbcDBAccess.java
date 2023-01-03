package com.crystal.jobs.utils;

public class JdbcDBAccess {

    private static JdbcDBAccess INSTANCE;
    public static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String DB_URL = "jdbc:mysql://localhost:3306/conference";
    public static final String DB_USER_NAME = "root";
    public static final String DB_PASSWORD = "Shanti2022!";
    private JdbcDBAccess() {
    }

    public static synchronized JdbcDBAccess getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new JdbcDBAccess();
        }
        return INSTANCE;
    }


}
