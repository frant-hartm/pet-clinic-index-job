package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ParsingException;
import org.example.jet.petclinic.PetClinicIndexJob.Owner;
import org.example.jet.petclinic.PetClinicIndexJob.Pet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OneToManyMapperTest {

    private OneToManyMapper<Owner, Pet> mapper = new OneToManyMapper<>(
            Owner.class,
            Pet.class
    );

    @Test
    public void when_mapOwner_then_shouldProduceOwner() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = mapper.mapState(1L, incomingOwner);

        assertThat(outgoingOwner).isSameAs(incomingOwner);
    }

    @Test
    public void when_mapPet_then_shouldProduceNothing() throws ParsingException {
        Owner outgoingOwner = mapper.mapState(1L, new Pet(100, "Samantha", 1));
        assertThat(outgoingOwner).isNull();
    }

    @Test
    public void mapOwnerAndPetShouldProduceOwnerWithPet() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "Coleman");
        mapper.mapState(1L, incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        Owner outgoingOwner = mapper.mapState(1L, pet);

        assertThat(outgoingOwner.pets).contains(pet);
    }

    @Test
    public void when_mapManyAndOne_then_shouldProduceOneWithMany() throws ParsingException {
        Pet pet = new Pet(100, "Samantha", 1);
        mapper.mapState(1L, pet);

        Owner incoming = new Owner(1, "Jean", "Coleman");
        Owner outgoing = mapper.mapState(1L, incoming);

        assertThat(outgoing.pets).contains(pet);
    }

    @Test
    public void when_mapOwnerWithPetAndUpdatedOwner_then_shouldProduceUpdatedOwnerWithPets() throws ParsingException {
        Owner incomingOwner = new Owner(1, "Jean", "ColemanColeman");
        mapper.mapState(1L, incomingOwner);

        Pet pet = new Pet(100, "Samantha", 1);
        mapper.mapState(1L, pet);

        Owner updatedOwner = new Owner(1, "Jean", "Coleman");
        Owner outgoingOwner = mapper.mapState(1L, updatedOwner);

        assertThat(outgoingOwner.lastName).isEqualTo("Coleman");
        assertThat(outgoingOwner.pets).contains(pet);
    }
}