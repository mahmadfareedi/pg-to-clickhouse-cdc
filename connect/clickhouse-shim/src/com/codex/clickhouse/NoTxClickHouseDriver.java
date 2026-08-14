package com.codex.clickhouse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Simple wrapper around the official ClickHouse JDBC driver that ignores
 * {@code setAutoCommit(false)} calls. The Confluent JDBC sink expects
 * transactional capabilities, but ClickHouse operates in autocommit-only mode.
 * This shim allows the connector to proceed while still delegating all other
 * behavior to the real driver.
 */
public final class NoTxClickHouseDriver implements Driver {
    private static final String SHIM_PREFIX = "jdbc:clickhouse+notx://";
    private final Driver delegate;

    public NoTxClickHouseDriver() throws SQLException {
        try {
            Class<?> driverClass = Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            delegate = (Driver) driverClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SQLException("Unable to load com.clickhouse.jdbc.ClickHouseDriver", e);
        }
    }

    private static String normalizeUrl(String url) {
        if (url != null && url.startsWith(SHIM_PREFIX)) {
            return "jdbc:clickhouse://" + url.substring(SHIM_PREFIX.length());
        }
        return url;
    }

    private Connection wrap(Connection connection) {
        if (connection == null) {
            return null;
        }

        InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if ("setAutoCommit".equals(method.getName())
                        && args != null
                        && args.length == 1
                        && args[0] instanceof Boolean
                        && Boolean.FALSE.equals(args[0])) {
                    // ClickHouse has no transactional mode; pretend it succeeded.
                    return null;
                }
                return method.invoke(connection, args);
            }
        };

        return (Connection) Proxy.newProxyInstance(
                connection.getClass().getClassLoader(),
                new Class<?>[] { Connection.class },
                handler);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Connection conn = delegate.connect(normalizeUrl(url), info);
        return wrap(conn);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url != null && url.startsWith(SHIM_PREFIX)) {
            return true;
        }
        return delegate.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return delegate.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }
}
