package org.example.jet.petclinic;

/**
 * Interface for the "one" side of a One-to-Many relationship
 *
 * @param <T>
 * @param <U>
 */
public interface One<T extends One<?, ?>, U extends Many<U>> {

    /**
     * Update this instance from a new version
     *
     * Note that this instance should keep all previous modifications, notably related records added via
     * {@link #merge(Many)}
     *
     * @param updatedOne new version of this
     */
    void update(T updatedOne);

    /**
     * Update this instance with an instance of "many" side of a One-to-Many relationship
     *
     * @param many an instance of "many"
     */
    void merge(U many);
}
