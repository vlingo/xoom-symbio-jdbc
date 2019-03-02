// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

/**
 * ReferenceObject an object identified primarily by its identity.  
 */
public interface ReferenceObject {
  /**
   * id is the generic representation of the identity that represents a reference object.
   * 
   * @return T inferred identity type of implementing persistent object.
   */
  <T> T id();
}
