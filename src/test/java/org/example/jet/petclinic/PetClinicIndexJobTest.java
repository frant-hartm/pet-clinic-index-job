package org.example.jet.petclinic;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.picocli.CommandLine;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public class PetClinicIndexJobTest extends JetTestSupport {

    private static final Logger log = LoggerFactory.getLogger(PetClinicIndexJobTest.class);

    private static final int MYSQL_PORT = 3306;
    private static final String PETCLINIC_INDEX = "petclinic-index";

    @Rule
    public MySQLContainer<?> mysql = new MySQLContainer<>("mysql")
            .withDatabaseName("petclinic")
            .withUsername("petclinic")
            .withPassword("petclinic")
            .withInitScript("schema.sql")
            .withExposedPorts(MYSQL_PORT);

    @Rule
    public ElasticsearchContainer elastic = new ElasticsearchContainer("elasticsearch:7.9.2")
            .withEnv("discovery.type", "single-node")
            .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
            .withExposedPorts(9200);

    private static RestHighLevelClient client;
    private static Job job;

    private final PetClinicIndexJob petClinicIndexJob = new PetClinicIndexJob();
    private JetInstance jet;

    @Before
    public void setUp() throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j2");
        addDebeziumPermissions();

        mysql.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("DOCKER")));
        elastic.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("DOCKER")));

        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create(elastic.getHttpHostAddress())
        ));

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(PETCLINIC_INDEX);
        createIndexRequest.mapping("{\n" +
                "  \"properties\": {\n" +
                "    \"first_name\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"copy_to\": \"search\"\n" +
                "    },\n" +
                "    \"last_name\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"copy_to\": \"search\"\n" +
                "    " +
                "},\n" +
                "    \"pets.name\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"copy_to\": \"search\"\n" +
                "    " +
                "},\n" +
                "    \"pets.visits.keywords\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"copy_to\": \"search\"\n" +
                "    " +
                "},\n" +
                "    \"search\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}\n", XContentType.JSON);
        client.indices().create(createIndexRequest, DEFAULT);

        Integer mysqlMappedPort = mysql.getMappedPort(MYSQL_PORT);

        new CommandLine(petClinicIndexJob).parseArgs(
                "--database-address", "localhost",
                "--database-port", mysqlMappedPort.toString(),
                "--database-user", "petclinic",
                "--database-password", "petclinic",
                "--elastic-host", elastic.getHttpHostAddress(),
                "--elastic-index", PETCLINIC_INDEX
        );

        jet = createJetMember();
        job = jet.newJob(petClinicIndexJob.pipeline());
    }

    @After
    public void after() throws Exception {
        jet.shutdown();
    }

    @Test
    public void whenInsertOwner_thenPipelineShouldWriteOwnerToElastic() {
        try (Connection connection = DriverManager.getConnection(mysql.getJdbcUrl(), "petclinic", "petclinic");
             Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO owners(first_name, last_name, address, city, telephone) VALUES " +
                            "('George', 'Franklin', '110 W. Liberty St. ', 'Madison', 6085551023)"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        await().atMost(10, SECONDS)
               .pollInterval(100, MILLISECONDS)
               .ignoreExceptions()
               .untilAsserted(() -> {
                   SearchResponse search = client.search(new SearchRequest(PETCLINIC_INDEX), DEFAULT);
                   SearchHits hits = search.getHits();
                   assertThat(hits).hasSize(1);

                   SearchHit hit = hits.getAt(0);
                   Map<String, Object> source = hit.getSourceAsMap();
                   assertThat(source)
                           .contains(
                                   entry("first_name", "George"),
                                   entry("last_name", "Franklin")
                           );

                   assertSingleResult("george");
                   assertSingleResult("franklin");
               });
    }

    private void assertSingleResult(String query) throws IOException {
        SearchResponse search;
        SearchHits hits;
        SearchRequest sr = new SearchRequest(PETCLINIC_INDEX);
        sr.source().query(QueryBuilders.matchQuery("search", query));
        search = client.search(sr, DEFAULT);
        hits = search.getHits();
        assertThat(hits).hasSize(1);
    }

    @Test
    public void whenInsertPet_thenPipelineShouldWritePetToElastic() {
        try (Connection connection = DriverManager.getConnection(mysql.getJdbcUrl(), "petclinic", "petclinic")) {
            int ownerId = createOwner(connection);
            createPet(connection, ownerId);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        await().atMost(10, SECONDS)
               .pollInterval(100, MILLISECONDS)
               .untilAsserted(() -> {
                   SearchResponse search = client.search(new SearchRequest(PETCLINIC_INDEX), DEFAULT);
                   SearchHits hits = search.getHits();
                   assertThat(hits).hasSize(1);

                   SearchHit hit = hits.getAt(0);
                   Map<String, Object> source = hit.getSourceAsMap();

                   List<Map<String, Object>> pets = (List<Map<String, Object>>) source.get("pets");

                   assertThat(pets).hasSize(1);
                   Map<String, Object> pet = pets.get(0);
                   assertThat(pet).contains(
                           entry("name", "Leo")
                   );

                   assertSingleResult("leo");
               });
    }

    @Test
    public void whenInsertVisit_thenPipelineShouldWriteVisitToElastic() {
        try (Connection connection = DriverManager.getConnection(mysql.getJdbcUrl(), "petclinic", "petclinic")) {
            int ownerId = createOwner(connection);
            int petId = createPet(connection, ownerId);
            createVisit(connection, petId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        await().atMost(10, SECONDS)
               .pollInterval(100, MILLISECONDS)
               .untilAsserted(() -> {
                   SearchResponse search = client.search(new SearchRequest(PETCLINIC_INDEX), DEFAULT);
                   SearchHits hits = search.getHits();
                   assertThat(hits).hasSize(1);

                   SearchHit hit = hits.getAt(0);
                   Map<String, Object> source = hit.getSourceAsMap();

                   List pets = (List) source.get("pets");
                   List visits = (List) ((Map<String, Object>) pets.get(0)).get("visits");
                   assertThat(visits).isNotEmpty();

                   assertSingleResult("nose");
               });
    }

    private int createOwner(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "INSERT INTO owners(first_name, last_name, address, city, telephone) VALUES " +
                            "('George', 'Franklin', '110 W. Liberty St. ', 'Madison', 6085551023)",
                    Statement.RETURN_GENERATED_KEYS
            );
            return returnKey(statement);
        }
    }

    private int createPet(Connection connection, int ownerId) throws SQLException {
        PreparedStatement petStatement = connection.prepareStatement(
                "INSERT INTO pets(name, birth_date, type_id, owner_id) VALUES " +
                        "(?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        petStatement.setString(1, "Leo");
        petStatement.setString(2, "2000-09-07");
        petStatement.setInt(3, 1);
        petStatement.setInt(4, ownerId);

        petStatement.executeUpdate();
        return returnKey(petStatement);
    }

    private int createVisit(Connection connection, int petId) throws SQLException {
        PreparedStatement visitStatement = connection.prepareStatement(
                "INSERT INTO visits(pet_id, visit_date, description) VALUES " +
                        "(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        visitStatement.setInt(1, petId);
        visitStatement.setString(2, "2010-03-04");
        visitStatement.setString(3, "Overall examination. Checked eyes, nose, legs. Gave rabies shot.");

        visitStatement.executeUpdate();
        return returnKey(visitStatement);
    }

    private int returnKey(Statement statement) throws SQLException {
        try (ResultSet keys = statement.getGeneratedKeys()) {
            if (keys.next()) {
                return keys.getInt(1);
            } else {
                throw new IllegalStateException("Should have returned 1 generated id");
            }
        }
    }

    public void addDebeziumPermissions() {
        try (Connection connection = DriverManager.getConnection(mysql.getJdbcUrl(), "root", "petclinic");
             Statement statement = connection.createStatement()) {

            statement.execute("ALTER USER petclinic IDENTIFIED WITH mysql_native_password BY 'petclinic'");
            statement.execute("GRANT RELOAD ON *.* TO 'petclinic'");
            statement.execute("GRANT REPLICATION CLIENT ON *.* TO 'petclinic'");
            statement.execute("GRANT REPLICATION SLAVE ON *.* TO 'petclinic'");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}