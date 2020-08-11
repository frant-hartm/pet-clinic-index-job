package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ParsingException;
import org.example.jet.petclinic.model.Owner;
import org.example.jet.petclinic.PetClinicIndexJob.OwnerMappingState;
import org.example.jet.petclinic.model.Pet;
import org.example.jet.petclinic.model.Visit;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

public class OwnerMappingStateTest {

    private OwnerMappingState state = new OwnerMappingState();

    @Test
    public void when_mapOwner_then_shouldProduceOwner() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = state.mapState(incomingOwner);

        assertThat(outgoingOwner).isSameAs(incomingOwner);
    }

    @Test
    public void when_mapPet_then_shouldProduceNothing() throws ParsingException {
        Owner outgoingOwner = state.mapState(new Pet(100, "Samantha", 1));
        assertThat(outgoingOwner).isNull();
    }


    @Test
    public void mapOwnerAndPetShouldProduceOwnerWithPet() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        state.mapState(incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        Owner outgoingOwner = state.mapState(pet);

        assertThat(outgoingOwner.pets).contains(pet);
    }


    @Test
    public void when_mapManyAndOne_then_shouldProduceOneWithMany() throws ParsingException {
        Pet pet = new Pet(100, "Samantha", 1);
        state.mapState(pet);

        Owner incoming = new Owner(1, "Jean", "Coleman");
        Owner outgoing = state.mapState(incoming);

        assertThat(outgoing.pets).contains(pet);
    }


    @Test
    public void when_mapOwnerWithPetAndUpdatedOwner_then_shouldProduceUpdatedOwnerWithPets() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "ColemanColeman");
        state.mapState(incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        state.mapState(pet);

        Owner updatedOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = state.mapState(updatedOwner);

        assertThat(outgoingOwner.lastName).isEqualTo("Coleman");
        assertThat(outgoingOwner.pets).contains(pet);
    }

    @Test
    public void ashouldMapVisitToDocument() throws Exception {

        Owner ownerRecord = ownerRecord();

        Owner document = state.mapState(ownerRecord);

        assertThat(document).isNotNull();
        assertThat(document.firstName).isEqualTo("Jean");
        assertThat(document.lastName).isEqualTo("Coleman");
        assertThat(document.id).isEqualTo(6);


        Pet petRecord = petRecord();

        document = state.mapState(petRecord);

        assertThat(document).isNotNull();
        assertThat(document.pets).hasSize(1);
        assertThat(document.pets.get(0).name).isEqualTo("Samantha");

        Visit visitRecord = visitRecord();

        document = state.mapState(visitRecord);

        assertThat(document.pets).isNotEmpty();
        assertThat(document.pets.get(0).visits).isNotEmpty();
    }

    @Test
    public void shouldMapVisitToDocument() throws Exception {
        OwnerMappingState state = new OwnerMappingState();

        Visit visitRecord = visitRecord();

        Owner owner = state.mapState(visitRecord);
        assertThat(owner).isNull();

        Owner ownerRecord = ownerRecord();
        owner = state.mapState(ownerRecord);

        assertThat(owner).isNotNull();
        assertThat(owner.firstName).isEqualTo("Jean");
        assertThat(owner.lastName).isEqualTo("Coleman");
        assertThat(owner.id).isEqualTo(6);
        assertThat(owner.pets).isNull();

        Pet petRecord = petRecord();

        owner = state.mapState(petRecord);

        assertThat(owner).isNotNull();
        assertThat(owner.pets).hasSize(1);
        assertThat(owner.pets.get(0).name).isEqualTo("Samantha");
        assertThat(owner.pets.get(0).visits.get(0).keywords).containsExactly("shot");
    }

    @NotNull
    private Owner ownerRecord() {
        return new Owner(6, "Jean", "Coleman");
    }

    @NotNull
    private Pet petRecord() {
        return new Pet(7, "Samantha", 6);
    }

    @NotNull
    private Visit visitRecord() {
        Visit visit = new Visit(7, "rabies shot");
        visit.setKeywords(newArrayList("shot"));
        return visit;
    }
}