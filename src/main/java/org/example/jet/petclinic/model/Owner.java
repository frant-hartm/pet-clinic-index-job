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

    public List<Pet> pets;

    // Used by Json deserialization
    public Owner() {
    }

    public Owner(Integer id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public void updateFrom(Owner newOwner) {
        firstName = newOwner.firstName;
        lastName = newOwner.lastName;
    }

    public void addPet(Pet newPet) {
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
