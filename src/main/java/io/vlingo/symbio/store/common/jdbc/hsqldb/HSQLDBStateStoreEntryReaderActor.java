package io.vlingo.symbio.store.common.jdbc.hsqldb;

import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.state.StateStoreEntryReader;

public class HSQLDBStateStoreEntryReaderActor<T> extends Actor implements StateStoreEntryReader<T> {
  private final Advice configuration;
  private long currentId;
  private final String name;

  public HSQLDBStateStoreEntryReaderActor(final Advice configuration, final String name) {
    this.configuration = configuration;
    this.name = name;
    this.currentId = 0;
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  public Completes<Entry<T>> readNext() {
    return null;
  }

  @Override
  public Completes<List<Entry<T>>> readNext(final int maximumEntries) {
    return null;
  }

  @Override
  public void rewind() {
    currentId = 0;
  }

  @Override
  public Completes<String> seekTo(final String id) {
    return null;
  }
}
