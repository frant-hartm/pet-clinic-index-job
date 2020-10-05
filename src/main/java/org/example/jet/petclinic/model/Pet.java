package org.example.jet.petclinic.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Pet implements Serializable {

    public Integer id;

    @JsonProperty("owner_id")
    public Integer ownerId;

    public String name;

    public List<Visit> visits = new ArrayList<>();

    public Pet() {
    }

    public Pet(Integer id) {
        this.id = id;
    }

    public Pet(Integer id, String name, Integer ownerId) {
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
    }

    public Pet addVisit(Visit newVisit) {
        Pet newPet = new Pet(id, name, ownerId);
        newPet.visits = new ArrayList<>(visits);
        newPet.visits.add(newVisit);
        return newPet;
    }

    public Pet update(Pet other) {
        this.visits = new ArrayList<>(other.visits);
        return this;
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
