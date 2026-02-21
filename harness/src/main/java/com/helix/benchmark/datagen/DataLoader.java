package com.helix.benchmark.datagen;

import com.helix.benchmark.config.DatabaseTarget;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DataLoader {
    private static final Logger log = LoggerFactory.getLogger(DataLoader.class);

    public static List<List<Document>> partition(List<Document> docs, int batchSize) {
        if (docs.isEmpty()) return List.of();
        List<List<Document>> batches = new ArrayList<>();
        for (int i = 0; i < docs.size(); i += batchSize) {
            batches.add(docs.subList(i, Math.min(i + batchSize, docs.size())));
        }
        return batches;
    }

    public static int batchSizeFor(DatabaseTarget target, int mongoBatchSize, int jdbcBatchSize) {
        return target.usesJdbc() ? jdbcBatchSize : mongoBatchSize;
    }

    public void loadToMongo(com.mongodb.client.MongoCollection<Document> collection,
                            List<Document> docs, int batchSize) {
        List<List<Document>> batches = partition(docs, batchSize);
        int loaded = 0;
        for (List<Document> batch : batches) {
            collection.insertMany(batch);
            loaded += batch.size();
            if (loaded % 10000 == 0) {
                log.info("Loaded {} / {} documents to {}", loaded, docs.size(),
                        collection.getNamespace().getCollectionName());
            }
        }
        log.info("Completed loading {} documents to {}",
                docs.size(), collection.getNamespace().getCollectionName());
    }

    public void loadToOracle(javax.sql.DataSource dataSource, String tableName,
                             List<Document> docs, int batchSize) throws Exception {
        List<List<Document>> batches = partition(docs, batchSize);
        String sql = "INSERT INTO " + tableName + " (data) VALUES (?)";
        int loaded = 0;
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            for (List<Document> batch : batches) {
                for (Document doc : batch) {
                    ps.setString(1, doc.toJson());
                    ps.addBatch();
                }
                ps.executeBatch();
                loaded += batch.size();
                if (loaded % 10000 == 0) {
                    log.info("Loaded {} / {} documents to {}", loaded, docs.size(), tableName);
                }
            }
            conn.commit();
        }
        log.info("Completed loading {} documents to {}", docs.size(), tableName);
    }
}
