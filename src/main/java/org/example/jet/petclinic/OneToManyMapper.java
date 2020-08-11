package org.example.jet.petclinic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Stateful mapper joining objects of One-to-Many relationship together.
 *
 * The most common use case is to join records of type T and U, on T's primary key and U's foreign key referencing T's
 * primary key.
 *
 * This implementation is roughly equivalent to a LEFT JOIN, but it could be extended to cover other types of joins as
 * well.
 *
 * Usage:
 * <pre>{@code StreamStage<Object> ownersAndPets = ...
 * StreamStage<Owner> owners = ownersAndPets.groupingKey(PetClinicIndexJob::getOwnerId)
 *   .mapStateful(
 *     () -> new OneToManyMapper<>(Owner.class, Pet.class),
 *     OneToManyMapper::mapState
 *   )}</pre
 *
 * @param <T>
 * @param <U>
 */
public class OneToManyMapper<T, U> {

    private final Class<?> tType;
    private final Class<?> uType;

    private final BiConsumer<T, T> updateOneFn;
    private final BiConsumer<T, U> mergeFn;

    Map<Long, T> idToOne = new HashMap<>();

    // Keep many instances for ones, which haven't arrived yet
    Map<Long, Collection<U>> idToMany = new HashMap<>();

    /**
     *
     * @param tType "one" type
     * @param uType "many" type
     * @param updateOneFn function to update instance of "one" with new version
     * @param mergeFn join "one" with an instance of "many"
     */
    public OneToManyMapper(
            Class<?> tType, Class<?> uType,
            BiConsumer<T, T> updateOneFn,
            BiConsumer<T, U> mergeFn
    ) {
        this.tType = tType;
        this.uType = uType;
        this.updateOneFn = updateOneFn;
        this.mergeFn = mergeFn;
    }

    public T mapState(Long key, Object item) {
        if (tType.isAssignableFrom(item.getClass())) {
            T one = (T) item;

            return idToOne.compute(key, (aKey, current) -> {
                if (current == null) {
                    // collect accumulated instances of many
                    Collection<U> many = idToMany.getOrDefault(key, Collections.emptyList());
                    for (U oneOfMany : many) {
                        mergeFn.accept(one, oneOfMany);
                    }
                    idToMany.remove(key);
                    return one;
                }

                if (current.equals(item)) {
                    return current;
                } else {
                    updateOneFn.accept(current, one);
                    return current;
                }
            });
        } else if (uType.isAssignableFrom(item.getClass())) {
            U oneOfMany = (U) item;
            return idToOne.compute(key, (aKey, current) -> {
                if (current == null) {
                    idToMany.compute(key, (aLong, many) -> {
                        if (many == null) {
                            many = new ArrayList<>();
                            many.add(oneOfMany);
                            return many;
                        } else {
                            many.add(oneOfMany);
                            return many;
                        }
                    });
                    return null;
                } else {
                    mergeFn.accept(current, oneOfMany);
                    return current;
                }
            });
        } else {
            throw new IllegalArgumentException("Unknown item type " + item.getClass());
        }
    }
}
