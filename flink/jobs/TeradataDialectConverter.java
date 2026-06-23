package com.teradata.flink;

import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.types.logical.RowType;

public class TeradataDialectConverter extends AbstractDialectConverter {
    private static final long serialVersionUID = 1L;

    public TeradataDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Teradata";
    }
}
