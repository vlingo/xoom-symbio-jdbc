/* Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved */
package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.symbio.store.object.ObjectStore;

/**
 * JPAObjectStore
 *
 * <p>Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved</p>
 *
 * @author mas
 * @since Feb 3, 2019
 */
public interface JPAObjectStore extends ObjectStore
{
    default void remove(  final Object persistentObject, final long removeId, final PersistResultInterest interest ) {
        remove( persistentObject, removeId, interest, null );
    }
    
    void remove( final Object persistentObject, final long removeId, final PersistResultInterest interest, final Object object );
}
