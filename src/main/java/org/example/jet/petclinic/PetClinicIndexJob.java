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
import com.hazelcast.jet.pipeline.StreamStage;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.jet.petclinic.rake.Rake;

import java.io.Serializable;
import java.util.ArrayList;
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

    private static final String[] TABLES = {"petclinic.owners", "petclinic.pets", "petclinic.visits"};

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

    private static Long getPetId(Object o) {
        if (o instanceof Pet) {
            Pet pet = (Pet) o;
            return Long.valueOf(pet.id);
        } else if (o instanceof Visit) {
            Visit visit = (Visit) o;
            return Long.valueOf(visit.petId);
        } else {
            throw new IllegalArgumentException("Unknown type " + o.getClass());
        }
    }

    private static Long getOwnerId(Object o) {
        if (o instanceof Owner) {
            Owner owner = (Owner) o;
            return Long.valueOf(owner.ownerId);
        } else if (o instanceof Pet) {
            Pet pet = (Pet) o;
            return Long.valueOf(pet.ownerId);
        } else {
            throw new IllegalArgumentException("Unknown type " + o.getClass());
        }
    }

    public Pipeline pipeline() {
        StreamSource<ChangeRecord> mysqlSource = MySqlCdcSources
                .mysql("mysql")
                .setDatabaseAddress(databaseAddress)
                .setDatabasePort(databasePort)
                .setDatabaseUser(databaseUser)
                .setDatabasePassword(databasePassword)
                .setClusterName(clusterName)
                .setDatabaseWhitelist(DATABASE)
                .setTableWhitelist(TABLES)
                .build();

        ServiceFactory<?, Rake> keywordService = ServiceFactories.sharedService((context) -> new Rake("en"));

        Sink<Owner> elasticSink = ElasticSinks.elastic(
                () -> RestClient.builder(HttpHost.create(elasticHost)),
                this::mapDocumentToElasticRequest
        );

        Pipeline p = Pipeline.create();
        StreamStage<Object> allRecords =
                p.readFrom(mysqlSource)
                 .withoutTimestamps()
                 .map(PetClinicIndexJob::mapChangeRecordToPOJO)
                 .mapUsingService(keywordService, PetClinicIndexJob::enrichWithKeywords);

        StreamStage<Pet> petWithVisits = allRecords
                .filter(o1 -> !(o1 instanceof Owner)) // Pets and Visits
                .groupingKey(PetClinicIndexJob::getPetId)
                .mapStateful(
                        () -> new OneToManyMapper<>(Pet.class, Visit.class, Pet::updateFrom, Pet::addVisit),
                        OneToManyMapper::mapState
                );

        allRecords.filter(o -> o instanceof Owner)
                  .merge(petWithVisits)
                  .groupingKey(PetClinicIndexJob::getOwnerId)
                  .mapStateful(
                          () -> new OneToManyMapper<>(Owner.class, Pet.class, Owner::updateFrom, Owner::addPet),
                          OneToManyMapper::mapState
                  )
                  .peek()
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

    private static Object enrichWithKeywords(Rake service, Object object) {

        if (object instanceof Visit) {
            Visit visit = (Visit) object;

            LinkedHashMap<String, Double> keywordsFromText = service.getKeywordsFromText(visit.description);
            List<String> keywords = keywordsFromText.keySet()
                                                    .stream()
                                                    .limit(5)
                                                    .collect(Collectors.toList());
            visit.setKeywords(keywords);
        }
        return object;
    }

    private DocWriteRequest<?> mapDocumentToElasticRequest(Owner document) throws Exception {
        return new UpdateRequest(elasticIndex, document.ownerId.toString())
                .doc(JsonUtil.toJson(document), XContentType.JSON)
                .docAsUpsert(true);
    }


    public static class Owner implements Serializable {

        public Integer ownerId;
        public String firstName;
        public String lastName;
        public List<Pet> pets;

        public Owner() {
        }

        public Owner(Integer ownerId, String firstName, String lastName) {
            this.ownerId = ownerId;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public void updateFrom(Owner newOwner) {
            firstName = newOwner.firstName;
            lastName = newOwner.lastName;
        }

        public void addPet(Pet newPet) {
            if (pets == null) {
                pets = new ArrayList<>();
            }

            for (Pet pet : pets) {
                if (pet.id.equals(newPet.id)) {
                    pet.name = newPet.name;
                    return;
                }
            }
            pets.add(newPet);
        }

        @Override
        public String toString() {
            return "Owner{" +
                    "ownerId=" + ownerId +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", petNames=" + pets +
                    '}';
        }
    }

    static class Pet {

        public Integer id;
        public Integer ownerId;
        public String name;

        public List<Visit> visits;

        public Pet() {
        }

        public Pet(Integer id, String name, Integer ownerId) {
            this.id = id;
            this.name = name;
            this.ownerId = ownerId;
        }

        public void addVisit(Visit newVisit) {
            if (visits == null) {
                visits = new ArrayList<>();
            }
            visits.add(newVisit);
        }

        public void updateFrom(Pet newPet) {
            this.visits = newPet.visits;
        }

        @Override
        public String toString() {
            return "Pet{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", visits='" + visits + '\'' +
                    '}';
        }
    }

    static class Visit {

        public Integer petId;
        public String description;
        public List<String> keywords;

        public Visit(Integer petId, String description) {
            this.petId = petId;
            this.description = description;
        }

        public void setKeywords(List<String> keywords) {
            this.keywords = keywords;
        }

        @Override public String toString() {
            return "Visit{" +
                    "petId=" + petId +
                    ", description='" + description + '\'' +
                    ", keywords=" + keywords +
                    '}';
        }
    }
}
