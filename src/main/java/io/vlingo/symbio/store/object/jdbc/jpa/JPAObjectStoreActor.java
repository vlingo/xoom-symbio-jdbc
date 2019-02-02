/* Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved */
package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;

import io.vlingo.actors.Actor;
import io.vlingo.common.Scheduled;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * JPAObjectStoreActor
 *
 * <p>Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved</p>
 *
 * @author mas
 * @since Feb 2, 2019
 */
public class JPAObjectStoreActor extends Actor implements ObjectStore, Scheduled
{
    private boolean closed;
    private final JPAObjectStoreDelegate delegate;
    
    public JPAObjectStoreActor( final JPAObjectStoreDelegate delegate )
    {
        this.delegate = delegate;
        this.closed = false;
    }

    /* @see io.vlingo.symbio.store.object.ObjectStore#close() */
    @Override
    public void close()
    {
        if ( !closed )
        {
            delegate.close();
            this.closed = true;
        }
    }

    /* @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
    @Override
    public void persist(Object persistentObject, long updateId, PersistResultInterest interest, Object object)
    {
        delegate.persist( persistentObject, updateId, interest, object );
    }

    /* @see io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
    @Override
    public void persistAll(Collection<Object> persistentObjects, long updateId, PersistResultInterest interest,
        Object object)
    {
        delegate.persistAll( persistentObjects, updateId, interest, object );
    }

    /* @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
    @Override
    public void queryAll(QueryExpression expression, QueryResultInterest interest, Object object)
    {
        delegate.queryAll( expression, interest, object );
    }

    /* @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
    @Override
    public void queryObject(QueryExpression expression, QueryResultInterest interest, Object object)
    {
        delegate.queryObject( expression, interest, object );
    }

    /* @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.store.object.PersistentObjectMapper) */
    @Override
    public void registerMapper(PersistentObjectMapper mapper)
    {
        delegate.registerMapper( mapper );
    }

    /* @see io.vlingo.common.Scheduled#intervalSignal(io.vlingo.common.Scheduled, java.lang.Object) */
    @Override
    public void intervalSignal(Scheduled scheduled, Object data)
    {
        delegate.timeoutCheck();
    }

}
