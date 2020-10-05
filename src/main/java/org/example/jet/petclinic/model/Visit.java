package org.example.jet.petclinic.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

public class Visit implements Serializable {

    @JsonProperty("pet_id")
    public Integer petId;
    public String description;
    public List<String> keywords;

    public Visit() {
    }

    public Visit(Integer petId, String description) {
        this.petId = petId;
        this.description = description;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public String toString() {
        return "Visit{" +
                "petId=" + petId +
                ", description='" + description + '\'' +
                ", keywords=" + keywords +
                '}';
    }
}
