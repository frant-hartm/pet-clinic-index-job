package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.elastic.ElasticSinks;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.picocli.CommandLine.Option;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.jet.petclinic.model.Owner;
import org.example.jet.petclinic.model.Pet;
import org.example.jet.petclinic.model.Visit;
import org.example.jet.petclinic.rake.Rake;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class PetClinicIndexJob implements Serializable {

    private static final String DATABASE = "petclinic";

    private static final String OWNERS_TABLE = "owners";
    private static final String PETS_TABLE = "pets";
    private static final String VISITS_TABLE = "visits";

    private static final String[] TABLE_WHITELIST = {"petclinic.owners", "petclinic.pets", "petclinic.visits"};

    @Option(names = {"-a", "--database-address"}, description = "database address")
    private String databaseAddress;

    @Option(names = {"-p", "--database-port"}, description = "database port")
    private int databasePort;

    @Option(names = {"-u", "--database-user"}, description = "database user")
    private String databaseUser;

    @Option(names = {"-s", "--database-password"}, description = "database password")
    private String databasePassword;

    @Option(names = {"-c", "--cluster-name"}, description = "database cluster name", defaultValue = "mysql-cluster")
    private String clusterName;

    @Option(names = {"-e", "--elastic-host"}, description = "elastic host")
    private String elasticHost;

    @Option(names = {"-i", "--elastic-index"}, description = "elastic index")
    private String elasticIndex;

    public Pipeline pipeline() {
        StreamSource<ChangeRecord> mysqlSource = MySqlCdcSources
                .mysql("mysql-debezium")
                .setDatabaseAddress(databaseAddress)
                .setDatabasePort(databasePort)
                .setDatabaseUser(databaseUser)
                .setDatabasePassword(databasePassword)
                .setClusterName(clusterName)
                .setDatabaseWhitelist(DATABASE)
                .setTableWhitelist(TABLE_WHITELIST)
                .build();

        ServiceFactory<?, Rake> keywordService = ServiceFactories.sharedService((context) -> new Rake("en"));

        Sink<Owner> elasticSink = ElasticSinks.elastic(
                () -> RestClient.builder(HttpHost.create(elasticHost)),
                this::mapDocumentToElasticRequest
        );

        Pipeline p = Pipeline.create();
        p.readFrom(mysqlSource)
         .withoutTimestamps()
         .map(PetClinicIndexJob::mapChangeRecordToPOJO).setName("mapChangeRecordToPOJO")
         .mapUsingService(keywordService, PetClinicIndexJob::enrichWithKeywords).setName("enrichWithKeywords")
         .mapStateful(JoiningState::new, JoiningState::mapState).setName("OwnerMappingState::mapState")
         .writeTo(elasticSink);

        return p;
    }

    private static Object mapChangeRecordToPOJO(ChangeRecord change) throws ParsingException {
        Map<String, Object> changeMap = change.value().toMap();

        switch (change.table()) {
            case OWNERS_TABLE: {
                Integer ownerId = (Integer) changeMap.get("id");
                String firstName = (String) changeMap.get("first_name");
                String lastName = (String) changeMap.get("last_name");

                return new Owner(ownerId, firstName, lastName);
            }

            case PETS_TABLE: {
                Integer petId = (Integer) changeMap.get("id");
                String name = (String) changeMap.get("name");
                Integer ownerId = (Integer) changeMap.get("owner_id");

                return new Pet(petId, name, ownerId);
            }

            case VISITS_TABLE:
                Integer petId = (Integer) changeMap.get("pet_id");
                String description = (String) changeMap.get("description");

                return new Visit(petId, description);

            default:
                throw new IllegalStateException("Unknown table " + change.table());
        }

    }

    private static Object enrichWithKeywords(Rake service, Object item) {
        if (item instanceof Visit) {
            Visit visit = (Visit) item;

            LinkedHashMap<String, Double> keywordsFromText = service.getKeywordsFromText(visit.description);
            List<String> keywords = keywordsFromText.keySet()
                                                    .stream()
                                                    .limit(5)
                                                    .collect(Collectors.toList());

            visit.setKeywords(keywords);

        }
        return item;
    }

    private DocWriteRequest<?> mapDocumentToElasticRequest(Owner document) throws Exception {
        return new UpdateRequest(elasticIndex, document.id.toString())
                .doc(JsonUtil.toJson(document), XContentType.JSON)
                .docAsUpsert(true);
    }

    static class JoiningState implements Serializable {

        Map<Integer, Owner> ownerMap = new HashMap<>();
        Map<Integer, Owner> petMap = new HashMap<>();

        Map<Integer, Pet> petIdToPetMap = new HashMap<>();

        public Owner mapState(Object item) {

            if (item instanceof Owner) {
                Owner owner = (Owner) item;

                return ownerMap.compute(owner.id, (key, currentOwner) -> {
                    if (currentOwner == null) {
                        return owner;
                    } else {
                        currentOwner.updateFrom(owner);
                        return currentOwner;
                    }
                });

            } else if (item instanceof Pet) {
                Pet pet = (Pet) item;

                Pet currentPet = petIdToPetMap.compute(pet.id, (key, aPet) -> {
                    if (aPet == null) {
                        return pet;
                    } else {
                        aPet.updateFrom(pet);
                        return aPet;
                    }
                });

                Owner owner = ownerMap.computeIfAbsent(currentPet.ownerId, key1 -> new Owner());
                owner.id = currentPet.ownerId;
                owner.addPet(currentPet);

                petMap.putIfAbsent(pet.id, owner);

                return documentIfOwnerSet(owner);
            } else if (item instanceof Visit) {
                Visit visit = (Visit) item;

                Pet pet = petIdToPetMap.computeIfAbsent(visit.petId, key -> new Pet(visit.petId));
                pet.addVisit(visit);

                if (pet.ownerId != null) {
                    return ownerMap.get(pet.ownerId);
                } else {
                    return null;
                }
            } else {
                throw new IllegalArgumentException("Unknown type " + item.getClass());
            }
        }

        private Owner documentIfOwnerSet(Owner document) {
            if (document.firstName == null) {
                //
                return null;
            } else {
                return document;
            }
        }
    }

}
