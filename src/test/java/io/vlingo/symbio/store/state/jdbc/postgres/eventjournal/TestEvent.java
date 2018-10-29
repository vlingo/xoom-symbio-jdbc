package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import java.util.Objects;

public class TestEvent {
    public final String id;
    public final long number;

    public TestEvent(String id, long number) {
        this.id = id;
        this.number = number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEvent that = (TestEvent) o;
        return number == that.number &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, number);
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "id='" + id + '\'' +
                ", number=" + number +
                '}';
    }
}
