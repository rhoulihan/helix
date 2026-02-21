package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;

import java.util.List;

public interface SchemaManager {
    List<String> getCollectionNames(SchemaModel model);
}
