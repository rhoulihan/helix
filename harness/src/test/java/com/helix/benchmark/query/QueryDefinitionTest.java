package com.helix.benchmark.query;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryDefinitionTest {

    @Test
    void shouldHaveNineQueries() {
        assertThat(QueryDefinition.values()).hasSize(9);
    }

    @Test
    void shouldHaveCorrectQueryNames() {
        assertThat(QueryDefinition.Q1.queryName()).isEqualTo("Q1");
        assertThat(QueryDefinition.Q9.queryName()).isEqualTo("Q9");
    }

    @Test
    void shouldHaveDescriptions() {
        for (QueryDefinition q : QueryDefinition.values()) {
            assertThat(q.description()).isNotBlank();
        }
    }

    @Test
    void shouldHaveCollectionAssignment() {
        assertThat(QueryDefinition.Q1.embeddedCollection()).isEqualTo("bookRoleInvestor");
        assertThat(QueryDefinition.Q2.embeddedCollection()).isEqualTo("bookRoleInvestor");
        assertThat(QueryDefinition.Q3.embeddedCollection()).isEqualTo("bookRoleInvestor");
        assertThat(QueryDefinition.Q4.embeddedCollection()).isEqualTo("bookRoleInvestor");
        assertThat(QueryDefinition.Q5.embeddedCollection()).isEqualTo("bookRoleGroup");
        assertThat(QueryDefinition.Q6.embeddedCollection()).isEqualTo("bookRoleGroup");
        assertThat(QueryDefinition.Q7.embeddedCollection()).isEqualTo("account");
        assertThat(QueryDefinition.Q8.embeddedCollection()).isEqualTo("advisor");
        assertThat(QueryDefinition.Q9.embeddedCollection()).isEqualTo("advisor");
    }

    @Test
    void shouldIndicateIfAggregationPipeline() {
        assertThat(QueryDefinition.Q1.isAggregation()).isTrue();
        assertThat(QueryDefinition.Q2.isAggregation()).isTrue();
        assertThat(QueryDefinition.Q3.isAggregation()).isTrue();
        assertThat(QueryDefinition.Q4.isAggregation()).isTrue();
        assertThat(QueryDefinition.Q5.isAggregation()).isFalse(); // find query
        assertThat(QueryDefinition.Q6.isAggregation()).isFalse();
        assertThat(QueryDefinition.Q7.isAggregation()).isFalse();
        assertThat(QueryDefinition.Q8.isAggregation()).isFalse();
        assertThat(QueryDefinition.Q9.isAggregation()).isFalse();
    }
}
