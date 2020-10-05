package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ParsingException;
import org.example.jet.petclinic.model.Owner;
import org.example.jet.petclinic.model.Pet;
import org.example.jet.petclinic.model.Visit;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

public class JoiningStateTest {

    private JoiningState state = new JoiningState();

    @Test
    public void when_mapOwner_then_shouldProduceOwner() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = state.join(incomingOwner);

        assertThat(outgoingOwner).isSameAs(incomingOwner);
    }

    @Test
    public void when_mapPet_then_shouldProduceNothing() throws ParsingException {
        Owner outgoingOwner = state.join(new Pet(100, "Samantha", 1));
        assertThat(outgoingOwner).isNull();
    }

    @Test
    public void when_joinVisit_then_shouldProduceNothing() {
        Owner outgoingOwner = state.join(new Visit(100, "rabbies shot"));
        assertThat(outgoingOwner).isNull();
    }

    @Test
    public void mapOwnerAndPetShouldProduceOwnerWithPet() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        Owner firstOutgoingOwner = state.join(incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        Owner outgoingOwner = state.join(pet);

        assertThat(outgoingOwner).isNotSameAs(firstOutgoingOwner);
        assertThat(outgoingOwner.pets).contains(pet);
    }

    @Test
    public void when_mapOwnerWithPetAndUpdatedOwner_then_shouldProduceUpdatedOwnerWithPets() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "ColemanColeman");
        Owner firstOutgoingOwner = state.join(incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        Owner secondOutgoingOwner = state.join(pet);

        Owner updatedOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = state.join(updatedOwner);

        assertThat(outgoingOwner).isNotSameAs(firstOutgoingOwner);
        assertThat(outgoingOwner).isNotSameAs(secondOutgoingOwner);

        assertThat(outgoingOwner.lastName).isEqualTo("Coleman");
        assertThat(outgoingOwner.pets).contains(pet);
    }

    @Test
    public void shouldJoinVisitToOwner() throws Exception {
        Owner ownerRecord = ownerRecord();

        Owner firstOutgoingOwner = state.join(ownerRecord);

        assertThat(firstOutgoingOwner).isNotNull();
        assertThat(firstOutgoingOwner.firstName).isEqualTo("Jean");
        assertThat(firstOutgoingOwner.lastName).isEqualTo("Coleman");
        assertThat(firstOutgoingOwner.id).isEqualTo(6);

        Pet petRecord = petRecord();

        Owner secondOutgoingOwner = state.join(petRecord);

        assertThat(secondOutgoingOwner).isNotNull();
        assertThat(secondOutgoingOwner.pets).hasSize(1);
        assertThat(secondOutgoingOwner.pets.get(0).name).isEqualTo("Samantha");

        Visit visitRecord = visitRecord();

        Owner outgoingOwner = state.join(visitRecord);

        assertThat(outgoingOwner).isNotSameAs(firstOutgoingOwner);
        assertThat(outgoingOwner).isNotSameAs(secondOutgoingOwner);

        assertThat(outgoingOwner.pets).isNotEmpty();
        assertThat(outgoingOwner.pets.get(0).visits).isNotEmpty();
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