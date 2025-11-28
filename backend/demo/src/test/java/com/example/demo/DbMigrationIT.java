package com.example.demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
@ActiveProfiles("test")
//@ContextConfiguration(initializers = DbMigrationIT.Initializer.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DbMigrationIT {

    // Use the pgvector image so CREATE EXTENSION vector works
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("pgvector/pgvector:pg16")
            .withDatabaseName("reading")
            .withUsername("app")
            .withPassword("app");

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext ctx) {
            TestPropertyValues.of(
                    "spring.datasource.url=" + pg.getJdbcUrl(),
                    "spring.datasource.username=" + pg.getUsername(),
                    "spring.datasource.password=" + pg.getPassword()
            ).applyTo(ctx.getEnvironment());
        }
    }

    @Autowired JdbcTemplate jdbc;

    @BeforeAll
    void started() { assertThat(pg.isRunning()).isTrue(); }

    @Test @DisplayName("pgvector extension is enabled")
    void vectorExtensionExists() {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM pg_extension WHERE extname = 'vector'", Integer.class);
        assertThat(count).isNotNull().isGreaterThan(0);
    }

    @Test @DisplayName("books table exists with embedding column")
    void booksTableHasEmbedding() {
        Integer cols = jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.columns " +
                        "WHERE table_schema = 'public' AND table_name = 'books' AND column_name = 'embedding'",
                Integer.class);
        assertThat(cols).isNotNull().isEqualTo(1);
    }
}
