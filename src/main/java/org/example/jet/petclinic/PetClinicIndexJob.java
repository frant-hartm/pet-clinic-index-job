package org.example.jet.petclinic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.function.PredicateEx;
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

    public Pipeline pipeline() throws ParsingException {
        StreamSource<ChangeRecord> mysqlSource = MySqlCdcSources
                .mysql("mysql")
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
        StreamStage<ChangeRecord> allRecords = p.readFrom(mysqlSource)
                                                .withoutTimestamps();

        var pets = allRecords.filter(table(PETS_TABLE))
                             .map(change -> (Object) change.value().toObject(Pet.class));

        var visits = allRecords.filter(table(VISITS_TABLE))
                               .map(change -> change.value().toObject(Visit.class))
                               .mapUsingService(keywordService, PetClinicIndexJob::enrichWithKeywords);

        StreamStage<Pet> petWithVisits = pets.merge(visits)
                                             .groupingKey(PetClinicIndexJob::getPetId)
                                             .mapStateful(
                                                     () -> new OneToManyMapper<>(Pet.class, Visit.class),
                                                     OneToManyMapper::mapState
                                             );

        allRecords.filter(table(OWNERS_TABLE))
                  .map(change -> (Object) change.value().toObject(Owner.class))
                  .merge(petWithVisits)
                  .groupingKey(PetClinicIndexJob::getOwnerId)
                  .mapStateful(
                          () -> new OneToManyMapper<>(Owner.class, Pet.class),
                          OneToManyMapper::mapState
                  )
                  .writeTo(elasticSink);

        return p;
    }

    private static PredicateEx<ChangeRecord> table(String table) throws ParsingException {
        return (changeRecord) -> changeRecord.table().equals(table);
    }

    private DocWriteRequest<?> mapDocumentToElasticRequest(Owner document) throws Exception {
        return new UpdateRequest(elasticIndex, document.id.toString())
                .doc(JsonUtil.toJson(document), XContentType.JSON)
                .docAsUpsert(true);
    }

    private static Visit enrichWithKeywords(Rake service, Visit visit) {
        LinkedHashMap<String, Double> keywordsFromText = service.getKeywordsFromText(visit.description);
        List<String> keywords = keywordsFromText.keySet()
                                                .stream()
                                                .limit(5)
                                                .collect(Collectors.toList());
        visit.setKeywords(keywords);
        return visit;
    }


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
            return Long.valueOf(owner.id);
        } else if (o instanceof Pet) {
            Pet pet = (Pet) o;
            return Long.valueOf(pet.ownerId);
        } else {
            throw new IllegalArgumentException("Unknown type " + o.getClass());
        }
    }

    public static class Owner implements Serializable, One<Owner, Pet> {

        public Integer id;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("last_name")
        public String lastName;

        public List<Pet> pets;

        // Used by Json deserialization
        public Owner() {
        }

        public Owner(Integer id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        @Override
        public void update(Owner updatedOne) {
            firstName = updatedOne.firstName;
            lastName = updatedOne.lastName;
        }

        @Override
        public void merge(Pet newPet) {
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
                    "ownerId=" + id +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", petNames=" + pets +
                    '}';
        }
    }

    static class Pet implements One<Pet, Visit>, Many<Pet> {


        public Integer id;

        @JsonProperty("owner_id")
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

        @Override
        public void update(Pet updatedOne) {
            this.visits = updatedOne.visits;
        }

        @Override
        public void merge(Visit newVisit) {
            if (visits == null) {
                visits = new ArrayList<>();
            }
            visits.add(newVisit);
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

    static class Visit implements Many<Visit> {

        @JsonProperty("pet_id")
        public Integer petId;
        public String description;
        public List<String> keywords;

        public Visit() {
        }

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
