package org.example.jet.petclinic.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class Pet {


    public Integer id;

    @JsonProperty("owner_id")
    public Integer ownerId;

    public String name;

    public List<Visit> visits;

    public Pet(Integer id) {
        this.id = id;
    }

    public Pet(Integer id, String name, Integer ownerId) {
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
    }

    public void addVisit(Visit newVisit) {
        if (visits == null) {
            visits = new ArrayList<>();
        }
        visits.add(newVisit);
    }

    public void updateFrom(Pet newPet) {
        this.name = newPet.name;
        this.ownerId = newPet.ownerId;
    }

    @Override
    public String toString() {
        return "Pet{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", visits='" + visits + '\'' +
                '}';
    }
}
