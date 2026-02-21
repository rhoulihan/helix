package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;

import java.util.Map;

public interface QueryExecutor {
    long execute(QueryDefinition query, SchemaModel model, Map<String, Object> params);
}
