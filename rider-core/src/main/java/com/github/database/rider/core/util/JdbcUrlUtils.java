package com.github.database.rider.core.util;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by pestano on 07/09/16.
 */
public class JdbcUrlUtils {

    public static boolean isMsSql(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "sqlserver");
    }

    public static boolean isHsql(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "hsqldb");
    }

    public static boolean isH2(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "h2");
    }

    public static boolean isMysql(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "mysql");
    }

    public static boolean isPostgre(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "postgresql");
    }

    public static boolean isOracle(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "oracle");
    }

    public static boolean isDB2(String jdbcUrlProtocol) {
        return hasKeywordInUrlProtocol(jdbcUrlProtocol, "db2");
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

    private static boolean hasKeywordInUrlProtocol(String jdbcUrlProtocol, String keyword) {
        return jdbcUrlProtocol != null && jdbcUrlProtocol.contains(keyword);
    }

}
