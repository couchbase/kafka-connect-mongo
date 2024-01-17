package io.debezium.connector.postgresql.connection;

import io.debezium.config.Configuration;
import io.debezium.relational.*;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.spi.schema.DataCollectionId;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PostgresTableFilters implements DataCollectionFilters {

    private Set<TableId> includedTables;

    private boolean useCatalogBeforeSchema;

    public PostgresTableFilters(String tableIncludeList, Tables.TableFilter systemTablesFilter, Selectors.TableIdToStringMapper tableIdMapper, boolean useCatalogBeforeSchema) {
        this.useCatalogBeforeSchema=useCatalogBeforeSchema;
        this.includedTables=parse(tableIncludeList);
    }

    Set<TableId> parse(String includeList){
       String[] includeStrings = includeList.split(",");
       Set<TableId> tables=new HashSet<>();

       for(String table :includeStrings){
           tables.add(TableId.parse(table,useCatalogBeforeSchema));
       }
       return tables;
    }

    @Override
    public Tables.TableFilter dataCollectionFilter() {
        return tableId -> includedTables.contains(tableId);
    }
}
