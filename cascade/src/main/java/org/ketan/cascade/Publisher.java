package org.ketan.cascade;

public interface Publisher {

    public abstract void publish(Object collaborator, Class<?> outputClass, String name, Object output);

}