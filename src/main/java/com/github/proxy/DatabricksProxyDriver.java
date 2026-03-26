package com.github.proxy;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.lang.reflect.Proxy;

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
            // 确保加载真实驱动
            Class.forName(REAL_DRIVER_CLASS);
            Driver realDriver = DriverManager.getDriver(realUrl);
            Connection realConn = realDriver.connect(realUrl, info);
            if (realConn == null) return null;
            return createProxyConnection(realConn);
        } catch (Exception e) {
            throw new SQLException("Proxy Driver Error: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    // 修复：必须返回真实驱动的 PropertyInfo，否则 FineBI 界面可能无法显示连接参数
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        String realUrl = url.replace(PREFIX, "jdbc:databricks:");
        try {
            return DriverManager.getDriver(realUrl).getPropertyInfo(realUrl, info);
        } catch (Exception e) {
            return new DriverPropertyInfo[0];
        }
    }

    @Override
    public int getMajorVersion() { return 1; }
    @Override
    public int getMinorVersion() { return 0; }
    @Override
    public boolean jdbcCompliant() { return false; }
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private Connection createProxyConnection(Connection conn) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class[]{Connection.class},
            (proxy, method, args) -> {
                // 拦截预编译 SQL
                if (("prepareStatement".equals(method.getName()) || "prepareCall".equals(method.getName()))
                    && args != null && args.length > 0 && args[0] instanceof String) {
                    args[0] = SqlRewriter.rewrite((String) args[0]);
                }
                
                Object result = method.invoke(conn, args);
                
                // 如果返回的是 Statement，继续代理它
                if (result instanceof PreparedStatement) {
                    return createProxyStatement((PreparedStatement) result, true);
                } else if (result instanceof Statement) {
                    return createProxyStatement((Statement) result, false);
                }
                return result;
            });
    }

    private Statement createProxyStatement(Object stmt, boolean isPrepared) {
        Class<?>[] interfaces = isPrepared 
            ? new Class[]{PreparedStatement.class} 
            : new Class[]{Statement.class};
            
        return (Statement) Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            interfaces,
            (proxy, method, args) -> {
                String name = method.getName();
                // 拦截直接执行的 SQL
                if (("executeQuery".equals(name) || "executeUpdate".equals(name) || "execute".equals(name) || "addBatch".equals(name)) 
                    && args != null && args.length > 0 && args[0] instanceof String) {
                    args[0] = SqlRewriter.rewrite((String) args[0]);
                }
                return method.invoke(stmt, args);
            });
    }
}
