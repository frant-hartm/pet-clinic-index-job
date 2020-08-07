package org.example.jet.petclinic;

import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.datamodel.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class OneToManyMapper<T, U> {

    private final Class<?> tType;
    private final Class<?> uType;

    Map<Long, Tuple2<T, Collection<U>>> idToTupleMap;

    public OneToManyMapper(Class<?> tType, Class<?> uType
    ) {
        this.tType = tType;
        this.uType = uType;
    }

    public Tuple2<T, Collection<U>> mapState(Long key, Object item) throws ParsingException {
        if (tType.isAssignableFrom(item.getClass())) {
            return idToTupleMap.compute(key, (aKey, tuple2) -> {
                if (tuple2 == null) {
                    return Tuple2.tuple2((T) item, new ArrayList<>());
                }

                if (tuple2.getKey().equals(item)) {
                    return tuple2;
                } else {
                    return Tuple2.tuple2((T) item, tuple2.f1());
                }
            });
        } else if (uType.isAssignableFrom(item.getClass())) {
            return idToTupleMap.compute(key, (aKey, tuple2) -> {
                if (tuple2 == null) {
                    Collection<U> many = new ArrayList<>();
                    many.add((U) item);
                    return Tuple2.tuple2(null, many);
                } else {
                    tuple2.f1().add((U) item);
                    return tuple2;
                }
            });
        } else {
            throw new IllegalArgumentException("Unknown item type " + item.getClass());
        }
    }
}
