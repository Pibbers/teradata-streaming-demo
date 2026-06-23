package com.teradata.flink;

import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;

public class TeradataFactory implements JdbcFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:teradata:");
    }

    @Override
    public JdbcDialect createDialect() {
        return new TeradataDialect();
    }

    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        throw new UnsupportedOperationException("Teradata JDBC Catalog is not supported");
    }
}
