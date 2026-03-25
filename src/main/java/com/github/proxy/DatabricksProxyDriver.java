package com.github.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;


public class DatabricksProxyDriver implements Driver {
    private static final String PREFIX = "jdbc:proxy:databricks:";
    private static final String REAL_DRIVER_CLASS = "com.databricks.client.jdbc.Driver";

    static {
        try {
            DriverManager.registerDriver(new DatabricksProxyDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        String realUrl = url.replace(PREFIX, "jdbc:databricks:");
        try {
            Class.forName(REAL_DRIVER_CLASS);
            Connection realConn = DriverManager.getConnection(realUrl, info);
            return createProxyConnection(realConn);
        } catch (ClassNotFoundException e) {
            throw new SQLException("Real Databricks Driver not found in classpath: " + REAL_DRIVER_CLASS);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    private Connection createProxyConnection(Connection conn) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class[]{Connection.class},
            (proxy, method, args) -> {
                // 拦截 prepareStatement
                if ("prepareStatement".equals(method.getName()) || "prepareCall".equals(method.getName())) {
                    args[0] = SqlRewriter.rewrite((String) args[0]);
                }
                Object result = method.invoke(conn, args);
                if (result instanceof Statement) {
                    return createProxyStatement((Statement) result);
                }
                return result;
            });
    }

    private Statement createProxyStatement(Statement stmt) {
        Class<?>[] interfaces = (stmt instanceof PreparedStatement) 
            ? new Class[]{PreparedStatement.class} 
            : new Class[]{Statement.class};
            
        return (Statement) Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            interfaces,
            (proxy, method, args) -> {
                // 拦截 executeQuery, executeUpdate, execute
                String name = method.getName();
                if (("executeQuery".equals(name) || "executeUpdate".equals(name) || "execute".equals(name)) 
                    && args != null && args.length > 0 && args[0] instanceof String) {
                    args[0] = SqlRewriter.rewrite((String) args[0]);
                }
                return method.invoke(stmt, args);
            });
    }

    // 实现接口的其他必要方法
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException { return new DriverPropertyInfo[0]; }
    public int getMajorVersion() { return 1; }
    public int getMinorVersion() { return 0; }
    public boolean jdbcCompliant() { return false; }
    public Logger getParentLogger() { return null; }
}
