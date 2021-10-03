package com.github.database.rider.core.util;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by pestano on 07/09/16.
 */
public class JdbcUrlUtils {

    public static boolean isMsSql(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "sqlserver");
    }

    public static boolean isHsql(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "hsqldb");
    }

    public static boolean isH2(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "h2");
    }

    public static boolean isMysql(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "mysql");
    }

    public static boolean isPostgre(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "postgresql");
    }

    public static boolean isOracle(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "oracle");
    }

    public static boolean isDB2(String driverName) {
        return hasKeywordInUrlProtocol(driverName, "db2");
    }

    public static String getJdbcUrlProtocol(Connection connection) {
        try {
            String jdbcUrl = connection.getMetaData().getURL();
            String cleanURI = jdbcUrl.substring(5);
            URI uri = URI.create(cleanURI);
            return uri.getScheme().toLowerCase();
        } catch (SQLException e) {
            throw new RuntimeException("Could not get JDBC URL from provided connection.",e);
        }
    }

    private static boolean hasKeywordInUrlProtocol(String driverName, String keyword) {
        return driverName != null && driverName.contains(keyword);
    }

}
