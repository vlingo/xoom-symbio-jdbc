// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa.model;

import java.time.LocalDateTime;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import com.google.gson.reflect.TypeToken;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.jdbc.jpa.model.converters.LocalDateTimeConverter;

/**
 * JPAEntry is an implementation of {@link io.vlingo.symbio.store.dispatch.Dispatchable} that is designed
 * to be persisted via the Java Persistence API
 */
@Entity(name = "Dispatchables")
@Table(name="tbl_vlingo_objectstore_dispatchables")
@NamedNativeQueries({
        @NamedNativeQuery(
                name = "Dispatchables.deleteByDispatchId",
                query  = "DELETE FROM tbl_vlingo_objectstore_dispatchables WHERE dispatch_id = ?"
        ),
})
@NamedQueries({
        @NamedQuery(
                name = "Dispatchables.getUnconfirmed",
                query  = "SELECT d FROM Dispatchables AS d where d.originatorId = :orignatorId ORDER BY d.createdOn ASC"
        )
})
public class JPADispatchable {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE)
  @Column(name = "id", updatable = false, nullable = false)
  private String id;

  @Column(name="dispatch_id", updatable = false, nullable = false)
  private String dispatchId;

  @Column(name="originator_id", updatable = false, nullable = false)
  private String originatorId;

  @Column(name="created_on", updatable = false, nullable = false)
  @Convert(converter= LocalDateTimeConverter.class)
  private LocalDateTime createdOn;

  @Column(name="state_id", updatable = false)
  private String stateId;

  @Column(name="state_type", updatable = false)
  private String stateType;

  @Column(name="state_type_version", updatable = false)
  private Integer stateTypeVersion;

  @Lob
  @Column(name="state_data", updatable = false)
  private String stateData;

  @Column(name="state_data_version", updatable = false)
  private Integer stateDataVersion;

  @Lob
  @Column(name="state_metadata", updatable = false)
  private String stateMetadata;

  @Lob
  @Column(name="entries", updatable = false)
  private String entries;


  public JPADispatchable() {
  }


  public JPADispatchable(final String id, final String dispatchId, final String originatorId,
          final LocalDateTime createdOn, final String stateId, final String stateType,
          final Integer stateTypeVersion, final String stateData,
          final Integer stateDataVersion, final String stateMetadata,
          final String entries) {
    this.id = id;
    this.dispatchId = dispatchId;
    this.originatorId = originatorId;
    this.createdOn = createdOn;
    this.stateId = stateId;
    this.stateType = stateType;
    this.stateTypeVersion = stateTypeVersion;
    this.stateData = stateData;
    this.stateDataVersion = stateDataVersion;
    this.stateMetadata = stateMetadata;
    this.entries = entries;
  }

  public String getId() {
    return id;
  }

  public String getDispatchId() {
    return dispatchId;
  }

  public String getOriginatorId() {
    return originatorId;
  }

  public LocalDateTime getCreatedOn() {
    return createdOn;
  }

  public String getStateId() {
    return stateId;
  }

  public String getStateType() {
    return stateType;
  }

  public Integer getStateTypeVersion() {
    return stateTypeVersion;
  }

  public String getStateData() {
    return stateData;
  }

  public Integer getStateDataVersion() {
    return stateDataVersion;
  }

  public String  getStateMetadata() {
    return stateMetadata;
  }

  public String getEntries() {
    return entries;
  }


  public static JPADispatchable fromDispatchable(final String originatorId, final Dispatchable<Entry<String>, State<?>> dispatchable) {
    final JPADispatchable jpaDispatchable = new JPADispatchable();
    jpaDispatchable.dispatchId = dispatchable.id();
    jpaDispatchable.createdOn = dispatchable.createdOn();
    jpaDispatchable.originatorId = originatorId;

    dispatchable
            .state()
            .ifPresent(state-> {
              jpaDispatchable.stateId = state.id;
              jpaDispatchable.stateType = state.type;
              jpaDispatchable.stateTypeVersion = state.typeVersion;
              jpaDispatchable.stateData = JsonSerialization.serialized(state.data);
              jpaDispatchable.stateDataVersion = state.dataVersion;
              jpaDispatchable.stateMetadata = JsonSerialization.serialized(state.metadata);
            });

    jpaDispatchable.entries = JsonSerialization.serialized(dispatchable.entries());

    return jpaDispatchable;
  }

  public static Dispatchable<Entry<String>, State<?>> toDispatchable(final JPADispatchable jpaDispatchable) {

    State<?> state = null;
    if (jpaDispatchable.stateId != null && jpaDispatchable.stateData !=null ){
      final Class<?> stateType;
      try {
        stateType = Class.forName(jpaDispatchable.stateType);
      } catch (final ClassNotFoundException e) {
        throw new IllegalStateException(e);
      }

      state = new State.TextState(
              jpaDispatchable.id, stateType , jpaDispatchable.stateTypeVersion,
              jpaDispatchable.stateData, jpaDispatchable.stateDataVersion,
              JsonSerialization.deserialized(jpaDispatchable.stateMetadata, Metadata.class)
      );
    }

    List<Entry<String>> entries = null;
    if (jpaDispatchable.entries !=null && !jpaDispatchable.entries.isEmpty()){
      entries = JsonSerialization.deserializedList(jpaDispatchable.entries, new TypeToken<List<BaseEntry.TextEntry>>(){}.getType());
    }

    return new Dispatchable<>(jpaDispatchable.dispatchId, jpaDispatchable.createdOn, state, entries);
  }
}
