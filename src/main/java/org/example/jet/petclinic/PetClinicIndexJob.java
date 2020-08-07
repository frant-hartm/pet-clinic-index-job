package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.datamodel.Tuple2;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.Collections.emptyList;

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

        Sink<Document> elasticSink = ElasticSinks.elastic(
                () -> RestClient.builder(HttpHost.create(elasticHost)),
                this::mapDocumentToElasticRequest
        );

        Pipeline p = Pipeline.create();
        StreamStage<Object> allRecords =
                p.readFrom(mysqlSource)
                 .withoutTimestamps()
                 .map((change) -> {
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

                 });

        StreamStage<Object> owners = allRecords.filter(o -> o instanceof Owner);
        StreamStage<Object> petsAndVisits = allRecords.filter(o -> !(o instanceof Owner));

        StreamStage<Tuple2<Pet, Collection<Visit>>> petToVisit = petsAndVisits.groupingKey(o -> {
            if (o instanceof Pet) {
                Pet pet = (Pet) o;
                return Long.valueOf(pet.id);
            } else if (o instanceof Visit) {
                Visit visit = (Visit) o;
                return Long.valueOf(visit.petId);
            } else {
                throw new IllegalArgumentException("Unknown type " + o.getClass());
            }
        }).mapStateful(() -> new OneToManyMapper<Pet, Visit>(Pet.class, Visit.class), OneToManyMapper::mapState);

        StreamStage<Object> ownersAndPets = owners.merge(petToVisit);

        StreamStage<Tuple2<Owner, Collection<Tuple2<Pet, Collection<Visit>>>>> result =
                ownersAndPets.groupingKey(
                        o -> {
                            if (o instanceof Owner) {
                                Owner owner = (Owner) o;
                                return Long.valueOf(owner.ownerId);
                            } else if (o instanceof Tuple2) {
                                Tuple2<Pet, Collection<Visit>> tuple2 = (Tuple2) o;
                                return Long.valueOf(tuple2.f0().ownerId);
                            } else {
                                throw new IllegalArgumentException("Unknown type " + o.getClass());
                            }
                        }).mapStateful(
                        () -> new OneToManyMapper<Owner, Tuple2<Pet, Collection<Visit>>>(Owner.class,
                                Tuple2.class)
                        ,
                        OneToManyMapper::mapState);

        return p;
    }

    private static Tuple2<ChangeRecord, List<String>> enrichWithKeywords(
            Rake service,
            ChangeRecord change
    ) throws ParsingException {

        Map<String, Object> map = change.value().toMap();
        Object desc = map.get("description");
        if (desc instanceof String) {
            String description = (String) desc;
            LinkedHashMap<String, Double> keywordsFromText = service.getKeywordsFromText(description);
            List<String> keywords = keywordsFromText.keySet()
                                                    .stream()
                                                    .limit(5)
                                                    .collect(Collectors.toList());
            return tuple2(change, keywords);
        } else {
            return tuple2(change, emptyList());
        }
    }

    private DocWriteRequest<?> mapDocumentToElasticRequest(Document document) throws Exception {
        return new UpdateRequest(elasticIndex, document.ownerId.toString())
                .doc(JsonUtil.toJson(document), XContentType.JSON)
                .docAsUpsert(true);
    }

    static class DocumentMappingState implements Serializable {


        Map<Integer, Document> ownerMap = new HashMap<>();
        Map<Integer, Document> petMap = new HashMap<>();

        public Document mapChange(Long ignored, Tuple2<ChangeRecord, List<String>> changeKeywordsTuple2) throws Exception {
            ChangeRecord change = changeKeywordsTuple2.f0();
            List<String> keywords = changeKeywordsTuple2.f1();

            Map<String, Object> changeMap = change.value().toMap();
            switch (change.table()) {
                case OWNERS_TABLE: {
                    Integer ownerId = (Integer) changeMap.get("id");

                    Document document = getDocument(ownerId);
                    document.updateName(changeMap);

                    return document;
                }

                case PETS_TABLE: {
                    Integer ownerId = (Integer) changeMap.get("owner_id");

                    Document document = getDocument(ownerId);
                    document.addPet(changeMap);

                    Integer petId = (Integer) changeMap.get("id");
                    addPet(petId, document);

                    return documentIfOwnerSet(document);
                }

                case VISITS_TABLE:
                    Integer petId = (Integer) changeMap.get("pet_id");

                    Document document = getDocumentByPet(petId);
                    document.addKeywords(keywords);

                    return documentIfOwnerSet(document);

                default:
                    throw new IllegalStateException("Unknown table " + change.table());
            }
        }

        private Document getDocument(Integer ownerId) {
            Document document = ownerMap.computeIfAbsent(ownerId, Document::new);
            document.ownerId = ownerId;
            return document;
        }

        private void addPet(Integer petId, Document document) {
            petMap.compute(petId, (pId, existingDoc) -> {
                if (existingDoc != null) {
                    document.addKeywords(existingDoc.keywords);
                }
                return document;
            });
        }

        private Document getDocumentByPet(Integer petId) {
            return petMap.computeIfAbsent(petId, id -> new Document());
        }

        private Document documentIfOwnerSet(Document document) {
            if (document.firstName == null) {
                //
                return null;
            } else {
                return document;
            }
        }
    }

    public static class Document implements Serializable {

        public Integer ownerId;
        public String firstName;
        public String lastName;
        public List<Pet> pets;
        public Set<String> keywords;

        public Document() {
        }

        public Document(Integer ownerId) {
            this.ownerId = ownerId;
        }

        public void updateName(Map<String, Object> changeMap) {
            firstName = (String) changeMap.get("first_name");
            lastName = (String) changeMap.get("last_name");
        }

        public void addPet(Map<String, Object> changeMap) {
            Pet newPet = new Pet((Integer) changeMap.get("id"), (String) changeMap.get("name"));

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

        public void addKeywords(Collection<String> keywords) {
            if (this.keywords == null) {
                this.keywords = new HashSet<>();
            }
            this.keywords.addAll(keywords);
        }

        @Override
        public String toString() {
            return "Document{" +
                    "ownerId=" + ownerId +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", petNames=" + pets +
                    ", keywords=" + keywords +
                    '}';
        }
    }

    static class Owner {

        public Integer ownerId;
        public String firstName;
        public String lastName;

        public Owner(Integer ownerId, String firstName, String lastName) {
            this.ownerId = ownerId;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        @Override public String toString() {
            return "Owner{" +
                    "ownerId=" + ownerId +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    '}';
        }
    }

    static class Pet {


        public Integer id;
        public Integer ownerId;
        public String name;

        public Pet(Integer id, String name, Integer ownerId) {
            this.id = id;
            this.name = name;
            this.ownerId = ownerId;
        }

        @Override
        public String toString() {
            return "Pet{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    static class Visit {
        public Integer petId;
        public String description;
        public List<String> keywords;

        public Visit(Integer petId, String description) {

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
