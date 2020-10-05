package org.example.jet.petclinic;

import org.example.jet.petclinic.model.Owner;
import org.example.jet.petclinic.model.Pet;
import org.example.jet.petclinic.model.Visit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

class JoiningState implements Serializable {

    Map<Integer, Owner> ownerIdToOwner = new HashMap<>();
    Map<Integer, Owner> petIdToOwner = new HashMap<>();

    Map<Integer, Pet> petIdToPet = new HashMap<>();

    public Owner join(Object item) {

        if (item instanceof Owner) {
            Owner owner = (Owner) item;

            return ownerIdToOwner.compute(owner.id, (key, currentOwner) -> {
                if (currentOwner == null) {
                    // There is no owner with this id, the incoming item is stored and returned
                    return owner;
                } else {
                    // There is an existing owner, update the incoming item with already collected data and return it
                    return owner.update(currentOwner);
                }
            });

        } else if (item instanceof Pet) {
            Pet pet = (Pet) item;

            Pet currentPet = petIdToPet.compute(pet.id, (key, aPet) -> {
                if (aPet == null) {
                    // There is no pet with this id, the incoming item is stored and returned
                    return pet;
                } else {
                    // There is an existing pet, update the incoming item with already collected data and return it
                    return pet.update(aPet);
                }
            });

            Owner petOwner = updatePetForOwner(currentPet);

            return ownerIfNameSet(petOwner);
        } else if (item instanceof Visit) {
            Visit visit = (Visit) item;

            Pet pet = petIdToPet.computeIfAbsent(visit.petId, key -> new Pet(visit.petId));
            Pet updatedPet = pet.addVisit(visit);

            if (updatedPet.ownerId != null) {
                return updatePetForOwner(updatedPet);
            } else {
                return null;
            }
        } else {
            throw new IllegalArgumentException("Unknown type " + item.getClass());
        }
    }

    private Owner updatePetForOwner(Pet pet) {
        Owner petOwner = ownerIdToOwner.compute(pet.ownerId, (ownerId, currentOwner) -> {
            if (currentOwner == null) {
                // There is no Owner yet for this ownerId, create a empty "shell" owner to put pets in
                Owner newOwner = new Owner(pet.ownerId, pet);
                newOwner.id = pet.ownerId;
                return newOwner;
            } else {
                // Owner exists, add a pet to it and replace
                return currentOwner.addPet(pet);
            }

        });

        petIdToOwner.putIfAbsent(pet.id, petOwner);
        return petOwner;
    }

    private Owner ownerIfNameSet(Owner owner) {
        if (owner.firstName == null) {
            //
            return null;
        } else {
            return owner;
        }
    }
}
