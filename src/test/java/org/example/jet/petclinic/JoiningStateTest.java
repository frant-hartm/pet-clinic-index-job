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

    private OneToManyJoinState<Owner, Pet> state = new OneToManyJoinState<>(
            Owner.class,
            Pet.class,
            Owner::update,
            Owner::addPet
    );

    @Test
    public void when_mapOwner_then_shouldProduceOwner() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = state.join(1L, incomingOwner);

        assertThat(outgoingOwner).isSameAs(incomingOwner);
    }

    @Test
    public void when_mapPet_then_shouldProduceNothing() throws ParsingException {
        Owner outgoingOwner = state.join(1L, new Pet(100, "Samantha", 1));
        assertThat(outgoingOwner).isNull();
    }

    @Test
    public void mapOwnerAndPetShouldProduceOwnerWithPet() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        Owner firstOutgoingOwner = state.join(1L, incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        Owner outgoingOwner = state.join(1L, pet);

        assertThat(outgoingOwner).isNotSameAs(firstOutgoingOwner);
        assertThat(outgoingOwner.pets).contains(pet);
    }

    @Test
    public void when_mapOwnerWithPetAndUpdatedOwner_then_shouldProduceUpdatedOwnerWithPets() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "ColemanColeman");
        Owner firstOutgoingOwner = state.join(1L, incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        Owner secondOutgoingOwner = state.join(1L, pet);

        Owner updatedOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = state.join(1L, updatedOwner);

        assertThat(outgoingOwner).isNotSameAs(firstOutgoingOwner);
        assertThat(outgoingOwner).isNotSameAs(secondOutgoingOwner);

        assertThat(outgoingOwner.lastName).isEqualTo("Coleman");
        assertThat(outgoingOwner.pets).contains(pet);
    }

    @Test
    public void shouldJoinVisitToOwner() throws Exception {
        Owner ownerRecord = ownerRecord();

        Owner firstOutgoingOwner = state.join(6L, ownerRecord);

        assertThat(firstOutgoingOwner).isNotNull();
        assertThat(firstOutgoingOwner.firstName).isEqualTo("Jean");
        assertThat(firstOutgoingOwner.lastName).isEqualTo("Coleman");
        assertThat(firstOutgoingOwner.id).isEqualTo(6);

        Pet petRecord = petRecord();

        Owner secondOutgoingOwner = state.join(6L, petRecord);

        assertThat(secondOutgoingOwner).isNotNull();
        assertThat(secondOutgoingOwner.pets).hasSize(1);
        assertThat(secondOutgoingOwner.pets.get(0).name).isEqualTo("Samantha");
    }

    @NotNull
    private Owner ownerRecord() {
        return new Owner(6, "Jean", "Coleman");
    }

    @NotNull
    private Pet petRecord() {
        return new Pet(7, "Samantha", 6);
    }

}