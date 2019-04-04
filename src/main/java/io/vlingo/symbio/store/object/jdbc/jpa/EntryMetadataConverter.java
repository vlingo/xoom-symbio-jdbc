// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Metadata;
/**
 * EntryMetadataConverter is responsible for converting instances of
 * {@link Metadata} to and from a {@link String} representation.
 */
@Converter(autoApply=true)
public class EntryMetadataConverter implements AttributeConverter<Metadata, String> {

  /* @see javax.persistence.AttributeConverter#convertToDatabaseColumn(java.lang.Object) */
  @Override
  public String convertToDatabaseColumn(Metadata attribute) {
    return JsonSerialization.serialized(attribute);
  }

  /* @see javax.persistence.AttributeConverter#convertToEntityAttribute(java.lang.Object) */
  @Override
  public Metadata convertToEntityAttribute(String dbData) {
    return JsonSerialization.deserialized(dbData, Metadata.class);
  }
}
