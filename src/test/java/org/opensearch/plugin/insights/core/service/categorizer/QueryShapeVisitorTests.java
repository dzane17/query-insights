/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public final class QueryShapeVisitorTests extends OpenSearchTestCase {
    public void testQueryShapeVisitor() {
        QueryBuilder builder = new BoolQueryBuilder().must(new TermQueryBuilder("foo", "bar"))
            .filter(new ConstantScoreQueryBuilder(new RangeQueryBuilder("timestamp").from("12345677").to("2345678")))
            .should(
                new BoolQueryBuilder().must(new MatchQueryBuilder("text", "this is some text"))
                    .mustNot(new RegexpQueryBuilder("color", "red.*"))
            )
            .must(new TermsQueryBuilder("genre", "action", "drama", "romance"));
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor(
            new QueryShapeGenerator(mock(ClusterService.class)),
            new HashMap<>(),
            null,
            false,
            false
        );
        builder.visit(shapeVisitor);
        assertEquals(
            "{\"type\":\"bool\",\"must\"[{\"type\":\"term\"},{\"type\":\"terms\"}],\"filter\"[{\"type\":\"constant_score\",\"filter\"[{\"type\":\"range\"}]}],\"should\"[{\"type\":\"bool\",\"must\"[{\"type\":\"match\"}],\"must_not\"[{\"type\":\"regexp\"}]}]}",
            shapeVisitor.toJson()
        );
    }
}
