package io.vlingo.symbio.store;

import io.vlingo.symbio.Source;

public class TestEvents {

  public static abstract class Event extends Source<Event> {
    @Override
    public boolean equals(final Object other) {
      if (other == null || other.getClass() != getClass()) {
        return false;
      }
      return true;
    }
  }

  public static final class Event1 extends Event { }

  public static final class Event2 extends Event { }

  public static final class Event3 extends Event { }
}
