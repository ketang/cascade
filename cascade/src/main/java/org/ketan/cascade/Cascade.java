package org.ketan.cascade;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class Cascade {
    //  TODO: logging should be per-instance
    private static final Logger LOGGER = Logger.getLogger(Cascade.class);

    class Result<C> {
        private final Class<C> outputClass;
        //  gotta make sure all code interacting with this can be null
        private final C output;
        private final String name;

        public Result(final Class<C> outputClass, final C output, final String name) {
            super();
            this.outputClass = outputClass;
            this.output = output;
            this.name = name;
        }

        public Class<C> getResultClass() {
            return outputClass;
        }

        public C getOutput() {
            return output;
        }

        public String getName() {
            return name;
        }
    }

    class SomeCollab {
        @Output(name="z") String doStuff(Cascade c, @Input(timeout=20, name="y") String y) {
            return "zorblox";
        }
    }

    class Invocation {
        private final Method method;
        private final Object[] arguments;

        public Invocation(final Method method, final Object[] arguments) {
            this.method = method;
            this.arguments = arguments;
        }

        public Method getMethod() {
            return method;
        }

        public Object[] getArguments() {
            return arguments;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Output { //  TODO: rename
        String name();
        boolean executeAfterTimeout() default false; //  continue executing even after timeout?
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    @interface Input {
        String name();
        int timeout() default Integer.MAX_VALUE;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Collaborator {
        String name();
    }

    /**
     * Marks a collaboration method
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface CollaborationMethod {
        //  intentionally empty
    }

    /**
     * Marks a fallback method to be used if execution of the collaboration method fails or isn't attempted due to time.
     * Must have the same return type and @Publishes as the {@link CollaborationMethod} (TODO: should this be at the {@link Collaborator} level?)
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface FallbackMethod {
        //  intentionally empty
    }

    interface Publisher {
        void publish(Object collaborator, Class<?> outputClass, String name, Object output);
    }

    class Collaboration {
        private final Object collaborator;
        private final Method method;

        public Collaboration(Object collaborator, Method method) {
            this.collaborator = collaborator;
            this.method = method;
        }

        public Object getCollaborator() {
            return collaborator;
        }

        public Method getMethod() {
            return method;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Publishes {
        String[] names();
        Class<?> classes();
    }

    /*
     * An incubator contains a list of collaborators and a timeout
     *
     * A Cascade is started (static factory method) using an executor, an Iterable of collaborators, a clock, and a timeout. This factory method constructs a
     * graph of dependencies using the return types, @Input, @Output, @Publishes etc. info, and kicks off the first set of ready collaborators. If no
     * collaborator is immediately ready, it is an error. All collaborators in the graph should be reachable; if there are unreachable collaborators, it is an
     * error. The thing running the cycle of work gets put into the executor first, from which point it triggers subsequent work using the same executor.
     *
     * TODO: each collaborator is assigned an arbitrary number
     *
     * TODO: each Cascade is assigned a unique String ID when it is initiated; collaborations are assigned their own IDs of the form $CascadeID.$NUM
     *
     * TODO: Nothing can take a Publisher argument without @Publishes
     *
     * As results are published to the Cascade, the collaborator arguments are resolved, ready collaborators are invoked, and the graph is pruned. At each
     * invocation point, the execution history of the collaborator is examined to predict the likelihood of the collaborator completing within the allotted
     * time. A random roll (weighted by user type) determines whether to attempt the method. If the method is skipped or the method somehow fails, it invokes
     * the FallbackMethod (if available).
     *
     * Once the timeout expires, no further collaborators are invoked unless executeAfterTimeout is true
     *
     * When the timeout expires, the results thus far are snapshotted and provided to the caller. Additional collaborators' results are collected and attached
     * to the Cascade. The caller may choose to retain the Cascade in memory in order to provide the results of collaborators asynchronously.
     *
     * TODO: a cascade should have a method to look up results by name, type, or collaborator number.
     *
     * TODO: what if a result is optional for one collaborator but required for another?  do we allow fast-fail?  seems like we need to get that result but allow the first collaborator to proceed without it.
     *
     * TODO: uid
     *
     * TODO: hard limit on when collaborators can be initiated, even if they are executeAfterTimeout (how to set?  per collaborator?  request global?)
     */

    class Edge {
        private final String outputName;
        private final Class outputType;
        private final Collaboration target;

        public Edge(final String outputName, final Class outputType, final Collaboration target) {
            this.outputName = outputName;
            this.outputType = outputType;
            this.target = target;
        }

        public String getOutputName() {
            return outputName;
        }

        public Class getOutputType() {
            return outputType;
        }

        public Collaboration getTarget() {
            return target;
        }
    }

    public void start(final Executor executor, final Iterable<Object> allCollaborators, final long startTime, final long finishBy) {

        

        executor.execute(new Runnable() {
            public void run() {
                // TODO: look through collaborators to ensure that each one has exactly one applicable method
                final List<Collaboration> collaborations = Lists.newArrayListWithCapacity(allCollaborators.size());
                for (final Object collaborator : allCollaborators) {
                    final Method[] methods = collaborator.getClass().getMethods();
                    Method found = null;
                    for (final Method method : methods) {
                        final Class<?>[] parameterTypes = method.getParameterTypes();
                        final CollaborationMethod cmAnnotation = method.getAnnotation(CollaborationMethod.class);
                        if (cmAnnotation == null) {
                            continue;
                        }
                        if (found != null) {
                            // TODO: error
                        }
                        found = method;
                    }
                    if (found == null) {
                        // TODO: error
                    }
                    collaborations.add(new Collaboration(collaborator, found));
                }

                final BlockingQueue<Result<?>> resultQueue = new LinkedBlockingDeque<Result<?>>(20);
                final Publisher publisher = new Publisher() {
                    // @Override
                    public void publish(final Object collaborator, final Class<?> outputClass, final String name, final Object output) {
                        // TODO: log the result
                        final Result<?> result = new Result(outputClass, output, name);
                        try {
                            resultQueue.put(result);
                        } catch (final InterruptedException e) {
                            LOGGER.error("Interrupted while putting result", e);
                        }
                    }
                };

                final List<Result<?>> results = Lists.newArrayListWithCapacity(10);
                results.add(new Result(Cascade.class, this, null));

                long timeToNext = 0;

                while (true) {
                    try {
                        if (timeToNext < 0) {
                            timeToNext = 0;
                        }
                        Result<?> result = resultQueue.poll(timeToNext, TimeUnit.MILLISECONDS);
                        if (result != null) {
                            results.add(result);
                            resultQueue.drainTo(results);
                        }
                    } catch (final InterruptedException e) {
                        // TODO
                    }

                    final long now = clock();
                    for (int i = 0; i < collaborations.size();) {
                        final Collaboration collaboration = collaborations.get(i);
                        final Object collaborator = collaboration.getCollaborator();
                        Invocation invocation = null;
                        final Method method = collaboration.getMethod();
                        final Class<?>[] parameterTypes = method.getParameterTypes();

                        final Result<?>[] matches = new Result[parameterTypes.length - 1];
                        for (int j = 0; j < parameterTypes.length; j++) {
                            final Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                            @Nullable
                            Input annotation = null;
                            for (int k = 0; k < parameterAnnotations[j].length; k++) {
                                if (Input.class.equals(parameterAnnotations[j][k].annotationType())) {
                                    annotation = (Input) parameterAnnotations[j][k];
                                    break;
                                }
                            }
                            boolean satisfied = false;

                            for (final Result<?> result : results) {
                                if (parameterTypes[j].isAssignableFrom(result.getClass())) {
                                    if ((annotation == null) || (annotation.name() == null) || (annotation.name().equals(result.getName()))) {
                                        if (matches[j] == null) {
                                            matches[j] = result;
                                            satisfied = true;
                                        } else {
                                            // TODO: error; ambiguous
                                        }
                                    }
                                }
                            }
                            satisfied = satisfied || ((annotation != null) && (annotation.timeout() + startTime < now)); // will go with null
                            if (satisfied) {
                                final Object[] arguments = new Object[parameterTypes.length];
                                for (int k = 0; k < arguments.length; k++) {
                                    if (matches[k] == null) {
                                        arguments[k] = null;
                                    } else {
                                        arguments[k] = matches[k].getOutput();
                                    }
                                }
                                invocation = new Invocation(method, arguments);
                            }
                        }
                        if (invocation == null) {
                            i++;
                        } else {
                            collaborations.remove(i);

                            final Output outputAnnotation = invocation.getMethod().getAnnotation(Output.class);

                            if ((finishBy > now) || ((outputAnnotation != null) && outputAnnotation.executeAfterTimeout())) {
                                invoke(executor, publisher, invocation.getMethod(), collaborator, invocation.getArguments());
                            }
                        }
                    }
                    int remainingToDo = 0;
                    for (final Collaboration collaboration : collaborations) {
                        Method method = collaboration.getMethod();
                        final Output outputAnnotation = method.getAnnotation(Output.class);
                        if ((outputAnnotation != null) && outputAnnotation.executeAfterTimeout()) {
                            remainingToDo++;
                        }

                        final Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                        for (int i = 0; i < parameterAnnotations.length; i++) {
                            for (int j = 0; j < parameterAnnotations[j].length; j++) {
                                if (parameterAnnotations[i][j].annotationType().equals(Input.class)) {
                                    final long requiredBy = startTime + ((Input) parameterAnnotations[i][j]).timeout();
                                    if (requiredBy < timeToNext) {
                                        timeToNext = requiredBy;
                                    }
                                }
                            }
                        }
                    }
                    if ((finishBy <= now) && (remainingToDo == 0)) {
                        break;
                    }
                }
            }
        });
    }

    private void invoke(final Executor executor, final Publisher publisher, final Method foundMethod, final Object collaborator, final Object[] arguments, final int timeout) {
        //  TODO: request regulator
        executor.execute(new Runnable() {
            public void run() {
                try {
                    //  TODO: log call
                    final Object result = foundMethod.invoke(collaborator, arguments);
                    //  TODO: log result (redundant with publishing)
                    String name = null;
                    final Output outputAnnotation = foundMethod.getAnnotation(Output.class);
                    if (outputAnnotation != null) {
                        name = outputAnnotation.name();
                    }
                    publisher.publish(collaborator, foundMethod.getReturnType(), name, result);
                } catch (final IllegalAccessException e) {
                    LOGGER.error(methodErrorString(foundMethod, collaborator, arguments), e);
                } catch (final IllegalArgumentException e) {
                    LOGGER.error(methodErrorString(foundMethod, collaborator, arguments), e);
                } catch (final InvocationTargetException e) {
                    LOGGER.error(methodErrorString(foundMethod, collaborator, arguments), e);
                }
            }
        });
    }


    private static String methodErrorString(Method foundMethod, Object collaborator, Object[] arguments) {
        return null;
    }

    //  TODO: make overridable
    private long clock() {
        return System.currentTimeMillis();
    }
}
