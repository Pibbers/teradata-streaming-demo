package com.teradata.flink;

import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TeradataDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "Teradata";
    }

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new TeradataDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return Arrays.stream(LogicalTypeRoot.values()).collect(Collectors.toSet());
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.teradata.jdbc.TeraDriver");
    }
}
