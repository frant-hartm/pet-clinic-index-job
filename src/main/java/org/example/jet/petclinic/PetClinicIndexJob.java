package org.example.jet.petclinic;

import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
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
import org.example.jet.petclinic.model.Owner;
import org.example.jet.petclinic.model.Pet;
import org.example.jet.petclinic.model.Visit;
import org.example.jet.petclinic.rake.Rake;

import java.io.Serializable;
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

    public JobConfig config() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("PetClinicIndexJob");
        jobConfig.addClasspathResource("src/main/resources/data/en.txt", "data/en.txt");
        jobConfig.addClass(Rake.class);
        jobConfig.addPackage("picocli");
        jobConfig.addClass(PetClinicIndexJob.class);
        jobConfig.addClass(OneToManyJoinState.class);
        return jobConfig;
    }

    public Pipeline pipeline() {
        StreamSource<ChangeRecord> mysqlSource = MySqlCdcSources
                .mysql("mysql-cdc")
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
                this::mapOwnerToElasticRequest
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
                                                     () -> new OneToManyJoinState<>(Pet.class,
                                                             Visit.class,
                                                             Pet::update,
                                                             Pet::addVisit),
                                                     OneToManyJoinState::join
                                             );


        allRecords.filter(table(OWNERS_TABLE))
                  .map(change -> (Object) change.value().toObject(Owner.class))
                  .merge(petWithVisits)
                  .groupingKey(PetClinicIndexJob::getOwnerId)
                  .mapStateful(
                          () -> new OneToManyJoinState<>(Owner.class, Pet.class, Owner::update, Owner::addPet),
                          OneToManyJoinState::join
                  )
                  .writeTo(elasticSink);

        return p;
    }

    private static PredicateEx<ChangeRecord> table(String table) {
        return (changeRecord) -> changeRecord.table().equals(table);
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

    private DocWriteRequest<?> mapOwnerToElasticRequest(Owner owner) throws Exception {
        return new UpdateRequest(elasticIndex, owner.id.toString())
                .doc(JsonUtil.toJson(owner), XContentType.JSON)
                .docAsUpsert(true);
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
}
