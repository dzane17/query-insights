/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.*;

public class S3Exporter implements QueryInsightsExporter {
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private final ClusterService clusterService;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final String id;

    /**
     * Constructor
     *
     * @param client         client instance
     * @param clusterService cluster service
     * @param id             exporter id
     */
    public S3Exporter(
        final Client client,
        final ClusterService clusterService,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final String id
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.id = id;
    }

    @Override
    public void export(List<SearchQueryRecord> records) {

    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * @return {@link BlobStoreRepository} referencing the repository
     * @throws RepositoryMissingException if repository is not registered or if {@link QueryInsightsSettings#S3_EXPORTER_BUCKET_NAME} is not set
     */
    private BlobStoreRepository getRepository() throws RepositoryMissingException {
        RepositoriesService repositoriesService = repositoriesServiceSupplier.get();
        assert repositoriesService != null;
        String vectorRepo = clusterService.getClusterSettings().get(S3_EXPORTER_BUCKET_NAME);
        if (vectorRepo == null || vectorRepo.isEmpty()) {
            throw new RepositoryMissingException(
                "Vector repository " + S3_EXPORTER_BUCKET_NAME.getKey() + " is not registered"
            );
        }
        final Repository repository = repositoriesService.repository(vectorRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        return (BlobStoreRepository) repository;
    }
}
