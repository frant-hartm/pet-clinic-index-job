package org.example.jet.petclinic.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Owner implements Serializable {

    public Integer id;

    @JsonProperty("first_name")
    public String firstName;

    @JsonProperty("last_name")
    public String lastName;

    public List<Pet> pets = new ArrayList<>();

    // Used by Json deserialization
    public Owner() {
    }

    public Owner(Integer id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public Owner(Integer ownerId, Pet pet) {
        this.id = ownerId;
        pets.add(pet);
    }

    public Owner update(Owner other) {
        pets = new ArrayList<>(other.pets);
        return this;
    }

    public Owner addPet(Pet newPet) {
        Owner newOwner = new Owner(id, firstName, lastName);

        boolean update = false;
        for (Pet pet : pets) {
            if (pet.id.equals(newPet.id)) {
                newOwner.pets.add(newPet);
                update = true;
                break;
            } else {
                newOwner.pets.add(pet);
            }
        }

        if (!update) {
            newOwner.pets.add(newPet);
        }
        return newOwner;
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
