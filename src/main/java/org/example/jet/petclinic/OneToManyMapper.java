package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ParsingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class OneToManyMapper<T, U> {

    private final Class<?> tType;
    private final Class<?> uType;

    private final BiConsumer<T, T> updateOneFn;
    private final BiConsumer<T, U> mergeFn;

    Map<Long, T> idToOne = new HashMap<>();
    Map<Long, Collection<U>> idToMany = new HashMap<>();

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

    public T mapState(Long key, Object item) throws ParsingException {
        if (tType.isAssignableFrom(item.getClass())) {
            T one = (T) item;

            return idToOne.compute(key, (aKey, current) -> {
                if (current == null) {
                    Collection<U> many = idToMany.getOrDefault(key, Collections.emptyList());
                    for (U oneOfMany : many) {
                        mergeFn.accept(one, oneOfMany);
                    }
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
