/* Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved */
package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.symbio.store.object.PersistentObject;

/**
 * Person
 *
 * <p>Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved</p>
 *
 * @author mas
 * @since Feb 1, 2019
 */
public class Person 
extends PersistentObject
{
    private static final long serialVersionUID = 1L;
    
    int age;
    long id;
    String name;
    
    public Person() {}
    
    public Person( long persistentId )
    {
        this.id = persistentId;
    }
    
    public Person( long persistentId, int anAge, String aName )
    {
        this( persistentId );
        this.age = anAge;
        this.name = aName;
    }
    
}
