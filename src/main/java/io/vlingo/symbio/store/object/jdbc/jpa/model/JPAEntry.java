// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa.model;

import java.time.LocalDate;
import java.util.Comparator;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.object.jdbc.jpa.model.converters.LocalDateConverter;
/**
 * JPAEntry is an implementation of {@link Entry} that is designed
 * to be persisted via the Java Persistence API
 */
@Entity
@Table(name="tbl_vlingo_objectstore_entry_journal")
public class JPAEntry implements Entry<String> {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE)
  @Column(name = "e_id", updatable = false, nullable = false)
  private long id;

  @Column(name="e_data", updatable = false, nullable = false)
  private String entryData;

  @Column(name="e_entry_version", updatable = false, nullable = false)
  private int entryVersion;

  @Column(name="e_metadata_value", updatable = false, nullable = false)
  private String metadataValue;

  @Column(name="e_metadata_op", updatable = false, nullable = false)
  private String metadataOp;

  @Column(name="e_type", updatable = false, nullable = false)
  private String type;

  @Column(name="e_type_version", updatable = false, nullable = false)
  private int typeVersion;

  @Column(name="e_timestamp", updatable = false, nullable = false)
  @Convert(converter= LocalDateConverter.class)
  private LocalDate entryTimestamp;

  public JPAEntry() {
    super();
  }

  public JPAEntry(final Entry<String> entry) {
    this.entryData = entry.entryData();
    this.entryVersion = entry.entryVersion();
    this.metadataValue = entry.metadata().value;
    this.metadataOp = entry.metadata().operation;
    this.type = entry.typeName();
    this.typeVersion = entry.typeVersion();
    this.entryTimestamp = LocalDate.now();
  }

  public JPAEntry(final Class<?> type, final int typeVersion, final String entryData, final Metadata metadata) {
    this(type, typeVersion, entryData, Entry.DefaultVersion, metadata);
  }

  public JPAEntry(final Class<?> type, final int typeVersion, final String entryData, final int entryVersion, final Metadata metadata) {
    if (type == null) throw new IllegalArgumentException("Entry type must not be null.");
    this.type = type.getName();
    if (typeVersion <= 0) throw new IllegalArgumentException("Entry typeVersion must be greater than 0.");
    this.typeVersion = typeVersion;
    if (entryData == null) throw new IllegalArgumentException("Entry entryData must not be null.");
    this.entryData = entryData;
    this.entryVersion = entryVersion;
    if (metadata == null) throw new IllegalArgumentException("Entry metadata must not be null.");
    this.metadataValue = metadata().value;
    this.metadataOp = metadata().operation;
    this.entryTimestamp = LocalDate.now();
  }

  public JPAEntry(final String id, final Class<?> type, final int typeVersion, final String entryData, final Metadata metadata) {
    this(id, type, typeVersion, entryData, Entry.DefaultVersion, metadata);
  }

  public JPAEntry(final String id, final Class<?> type, final int typeVersion, final String entryData, final int entryVersion, final Metadata metadata) {
    if (id == null) throw new IllegalArgumentException("Entry id must not be null.");
    this.id = Long.parseLong(id);
    if (type == null) throw new IllegalArgumentException("Entry type must not be null.");
    this.type = type.getName();
    if (typeVersion <= 0) throw new IllegalArgumentException("Entry typeVersion must be greater than 0.");
    this.typeVersion = typeVersion;
    if (entryData == null) throw new IllegalArgumentException("Entry entryData must not be null.");
    this.entryData = entryData;
    this.entryVersion = entryVersion;
    if (metadata == null) throw new IllegalArgumentException("Entry metadata must not be null.");
    this.metadataValue = metadata().value;
    this.metadataOp = metadata().operation;
    this.entryTimestamp = LocalDate.now();
  }

  /* @see io.vlingo.symbio.Entry#id() */
  @Override
  public String id() {
    return String.valueOf(id);
  }

  public LocalDate entryTimestamp() {
    return entryTimestamp;
  }

  /* @see io.vlingo.symbio.Entry#entryData() */
  @Override
  public String entryData() {
    return entryData;
  }

  /* @see io.vlingo.symbio.Entry#entryVersion() */
  @Override
  public int entryVersion() {
    return entryVersion;
  }

  /* @see io.vlingo.symbio.Entry#metadata() */
  @Override
  public Metadata metadata() {
    return Metadata.with(metadataValue, metadataOp);
  }

  /* @see io.vlingo.symbio.Entry#type() */
  @Override
  public String typeName() {
    return type;
  }

  /* @see io.vlingo.symbio.Entry#typeVersion() */
  @Override
  public int typeVersion() {
    return typeVersion;
  }

  /* @see io.vlingo.symbio.Entry#hasMetadata() */
  @Override
  public boolean hasMetadata() {
    return !metadataValue.isEmpty();
  }

  /* @see io.vlingo.symbio.Entry#isEmpty() */
  @Override
  public boolean isEmpty() {
    return entryData.isEmpty();
  }

  /* @see io.vlingo.symbio.Entry#isNull() */
  @Override
  public boolean isNull() {
    return false;
  }

  /* @see io.vlingo.symbio.Entry#typed() */
  @SuppressWarnings("unchecked")
  @Override
  public <C> Class<C> typed() {
    try {
      return (Class<C>) Class.forName(type);
    } catch (final Exception e) {
      throw new IllegalStateException("Cannot get class for type: " + type);
    }
  }

  /* @see java.lang.Comparable#compareTo(java.lang.Object) */
  @Override
  public int compareTo(final Entry<String> other) {
    final JPAEntry that = (JPAEntry) other;
    return Comparator
      .comparing((JPAEntry e) -> e.id)
      .thenComparing(e -> e.entryTimestamp)
      .thenComparing(e -> e.entryData)
      .thenComparing(e -> e.entryVersion)
      .thenComparing(e -> e.type)
      .thenComparingInt(e -> e.typeVersion)
      .thenComparing(e -> e.metadataValue)
      .thenComparing(e -> e.metadataOp)
      .compare(this, that);
  }

  /* @see java.lang.Object#toString() */
  @Override
  public String toString() {
    return new StringBuilder()
      .append("JPAEntry(")
      .append("id=").append(id)
      .append(", entryTimestamp=").append(entryTimestamp)
      .append(", entryData=").append(entryData)
      .append(", metadata=").append(metadataValue).append(",").append(metadataOp)
      .append(", type=").append(type)
      .append(", typeVersion=").append(typeVersion)
      .append(")")
      .toString();
  }

  @Override
  public Entry<String> withId(final String id) {
    return new JPAEntry(id, typed(), typeVersion, entryData, Metadata.with(metadataValue, metadataOp));
  }

  public void __internal__setId(final String id) {
    this.id = Long.parseLong(id);
  }
}
