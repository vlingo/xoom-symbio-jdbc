// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa;

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
/**
 * JPAEntry is an implementation of {@link Entry} that is designed
 * to be persisted via the Java Persistence API
 */
@Entity
@Table(name="tbl_objectstore_event_journal")
public class JPAEntry implements Entry<String> {
  
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE)
  @Column(name = "id", updatable = false, nullable = false)
  private String id;

  @Column(name="entry_timestamp", updatable = false, nullable = false)
  @Convert(converter=LocalDateConverter.class)
  private LocalDate entryTimestamp;
  
  @Column(name="entry_data", updatable = false, nullable = false)
  private String entryData;
  
  @Column(name="metadata", updatable = false, nullable = false)
  @Convert(converter=EntryMetadataConverter.class)
  private Metadata metadata;

  @Column(name="entry_type", updatable = false, nullable = false)
  private String type;

  @Column(name="entry_type_version", updatable = false, nullable = false)
  private int typeVersion;
  
  public JPAEntry() {
    super();
  }
  
  public JPAEntry(final Entry<String> entry) {
    this.entryTimestamp = LocalDate.now();
    this.entryData = entry.entryData();
    this.metadata = entry.metadata();
    this.type = entry.type();
    this.typeVersion = entry.typeVersion();
  }
  
  public JPAEntry(final Class<?> type, final int typeVersion, final String entryData, final Metadata metadata) {
    this.entryTimestamp = LocalDate.now();
    if (type == null) throw new IllegalArgumentException("Entry type must not be null.");
    this.type = type.getName();
    if (typeVersion <= 0) throw new IllegalArgumentException("Entry typeVersion must be greater than 0.");
    this.typeVersion = typeVersion;
    if (entryData == null) throw new IllegalArgumentException("Entry entryData must not be null.");
    this.entryData = entryData;
    if (metadata == null) throw new IllegalArgumentException("Entry metadata must not be null.");
    this.metadata = metadata;
  }
  
  public JPAEntry(final String id, final Class<?> type, final int typeVersion, final String entryData, final Metadata metadata) {
    if (id == null) throw new IllegalArgumentException("Entry id must not be null.");
    this.id = id;
    this.entryTimestamp = LocalDate.now();
    if (type == null) throw new IllegalArgumentException("Entry type must not be null.");
    this.type = type.getName();
    if (typeVersion <= 0) throw new IllegalArgumentException("Entry typeVersion must be greater than 0.");
    this.typeVersion = typeVersion;
    if (entryData == null) throw new IllegalArgumentException("Entry entryData must not be null.");
    this.entryData = entryData;
    if (metadata == null) throw new IllegalArgumentException("Entry metadata must not be null.");
    this.metadata = metadata;
  }

  /* @see io.vlingo.symbio.Entry#id() */
  @Override
  public String id() {
    return id;
  }
  
  public LocalDate entryTimestamp() {
    return entryTimestamp;
  }

  /* @see io.vlingo.symbio.Entry#entryData() */
  @Override
  public String entryData() {
    return entryData;
  }

  /* @see io.vlingo.symbio.Entry#metadata() */
  @Override
  public Metadata metadata() {
    return metadata;
  }

  /* @see io.vlingo.symbio.Entry#type() */
  @Override
  public String type() {
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
    return !metadata.isEmpty();
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
    } catch (Exception e) {
      throw new IllegalStateException("Cannot get class for type: " + type);
    }
  }

  /* @see java.lang.Comparable#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Entry<String> other) {
    final JPAEntry that = (JPAEntry) other;
    return Comparator
      .comparing((JPAEntry e) -> e.id)
      .thenComparing(e -> e.entryTimestamp)
      .thenComparing(e -> e.entryData)
      .thenComparing(e -> e.type)
      .thenComparingInt(e -> e.typeVersion)
      .thenComparing(e -> e.metadata)
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
      .append(", metadata=").append(metadata)
      .append(", type=").append(type)
      .append(", typeVersion=").append(typeVersion)
      .append(")")
      .toString();
  }

}
