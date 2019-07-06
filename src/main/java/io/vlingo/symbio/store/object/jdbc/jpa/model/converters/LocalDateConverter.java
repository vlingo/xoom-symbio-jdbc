// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa.model.converters;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.sql.Date;
import java.time.LocalDate;
/**
 * LocalDateTimeConverter
 */
@Converter(autoApply = true)
public class LocalDateConverter implements AttributeConverter<LocalDate, Date> {

  /* @see javax.persistence.AttributeConverter#convertToDatabaseColumn(java.lang.Object) */
  @Override
  public Date convertToDatabaseColumn(LocalDate datetime) {
    return Date.valueOf(datetime);
  }

  /* @see javax.persistence.AttributeConverter#convertToEntityAttribute(java.lang.Object) */
  @Override
  public LocalDate convertToEntityAttribute(Date date) {
    return date.toLocalDate();
  }
}
