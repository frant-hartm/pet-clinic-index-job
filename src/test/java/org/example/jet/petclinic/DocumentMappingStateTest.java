package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.json.JsonUtil;
import org.example.jet.petclinic.PetClinicIndexJob.Document;
import org.example.jet.petclinic.PetClinicIndexJob.DocumentMappingState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.emptyList;
import static org.assertj.core.util.Lists.newArrayList;

public class DocumentMappingStateTest {

    @Test
    public void aashouldMapVisitToDocument() throws Exception {
        DocumentMappingState state = new DocumentMappingState();

        ChangeRecordImpl ownerRecord = ownerRecord();

        Document document = state.mapChange(0L, Tuple2.tuple2(ownerRecord, emptyList()));

        assertThat(document).isNotNull();
        assertThat(document.firstName).isEqualTo("Jean");
        assertThat(document.lastName).isEqualTo("Coleman");
        assertThat(document.ownerId).isEqualTo(6);


        ChangeRecordImpl petRecord = petRecord();

        document = state.mapChange(0L, Tuple2.tuple2(petRecord, emptyList()));

        assertThat(document).isNotNull();
        assertThat(document.pets).hasSize(1);
        assertThat(document.pets.get(0).name).isEqualTo("Samantha");

        ChangeRecordImpl visitRecord = visitRecord();

        document = state.mapChange(0L, Tuple2.tuple2(visitRecord, newArrayList("shot")));

        assertThat(document.keywords).containsExactly("shot");
    }

    @Test
    public void shouldMapVisitToDocument() throws Exception {
        DocumentMappingState state = new DocumentMappingState();

        ChangeRecordImpl visitRecord = visitRecord();

        Document document = state.mapChange(0L, Tuple2.tuple2(visitRecord, newArrayList("shot")));
        assertThat(document).isNull();

        ChangeRecordImpl ownerRecord = ownerRecord();
        document = state.mapChange(0L, Tuple2.tuple2(ownerRecord, emptyList()));

        assertThat(document).isNotNull();
        assertThat(document.firstName).isEqualTo("Jean");
        assertThat(document.lastName).isEqualTo("Coleman");
        assertThat(document.ownerId).isEqualTo(6);

        ChangeRecordImpl petRecord = petRecord();

        document = state.mapChange(0L, Tuple2.tuple2(petRecord, emptyList()));

        assertThat(document).isNotNull();
        assertThat(document.pets).hasSize(1);
        assertThat(document.pets.get(0).name).isEqualTo("Samantha");
        assertThat(document.keywords).containsExactly("shot");
    }

    @NotNull private ChangeRecordImpl petRecord() throws IOException {
        return new ChangeRecordImpl(0, 0,
                JsonUtil.toJson(Map.of("id", 7)),
                JsonUtil.toJson(Map.of(
                        "__table", "pets",
                        "id", 7,
                        "name", "Samantha",
                        "birth_date", "1995-09-04",
                        "type_id", 1,
                        "owner_id", 6
                ))
        );
    }

    @NotNull private ChangeRecordImpl ownerRecord() throws IOException {
        return new ChangeRecordImpl(0, 0,
                JsonUtil.toJson(Map.of("id", 6)),
                JsonUtil.toJson(Map.of(
                        "__table", "owners",
                        "id", 6,
                        "first_name", "Jean",
                        "last_name", "Coleman",
                        "address", "105 N. Lake St.",
                        "city", "Monona",
                        "telephone", 6085552654L
                ))
        );
    }

    @NotNull private ChangeRecordImpl visitRecord() throws IOException {
        return new ChangeRecordImpl(0, 0,
                JsonUtil.toJson(Map.of("id", 1)),
                JsonUtil.toJson(Map.of(
                        "__table", "visits",
                        "id", 1,
                        "pet_id", 7,
                        "visit_date", "2010-03-04",
                        "description", "rabies shot"
                ))
        );
    }
}