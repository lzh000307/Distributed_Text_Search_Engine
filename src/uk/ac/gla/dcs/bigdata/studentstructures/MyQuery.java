package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.Objects;

public class MyQuery {
    Query query;

    public MyQuery(Query query) {
        this.query = query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyQuery myQuery = (MyQuery) o;
        return Objects.equals(query.getOriginalQuery(), myQuery.query.getOriginalQuery());
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }
}

