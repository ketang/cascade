package org.ketan.cascade;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Cascade implements Publisher {
    private static final String RESULT_PUBLISHER = "publisher";
    private static final String RESULT_ID = "id";
    //  TODO: logging should be per-instance
    private static final Logger LOGGER = Logger.getLogger(Cascade.class);

    class SomeCollab {
        @Output(name="z") String doStuff(Publisher c, @Input(timeout=20, name="y") String y) {
            return "zorblox";
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

    class Collaboration {
        private final Object collaborator;
        private final Method method;
        private final Object[] parameters;
        private final boolean[] resolved;
        private final long deadline;

        public Collaboration(final Object collaborator, final Method method, final long deadline) {
            this.collaborator = collaborator;
            this.method = method;
            this.parameters = new Object[method.getParameterTypes().length];
            this.resolved = new boolean[method.getParameterTypes().length];
            this.deadline = deadline;
        }

        public Object getCollaborator() {
            return collaborator;
        }

        public Method getMethod() {
            return method;
        }

        public void resolveParameter(final int index, final Object parameter) {
            this.parameters[index] = parameter;
            this.resolved[index] = true;
        }

        public boolean allResolved() {
            for (int i = 0; i < this.resolved.length; i++) {
                if (! this.resolved[i]) {
                    return false;
                }
            }
            return true;
        }

        public Object[] getParameters() {
            return parameters;
        }

        public long getDeadline() {
            return deadline;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Publishes {
        String[] names();
        Class<?>[] classes();
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

    class CollaboratorParameter {
        private final Collaboration collaboration;
        private final int parameterIndex;
        public CollaboratorParameter(Collaboration collaboration, int parameterIndex) {
            this.collaboration = collaboration;
            this.parameterIndex = parameterIndex;
        }

        public Collaboration getCollaboration() {
            return collaboration;
        }

        public int getParameterIndex() {
            return parameterIndex;
        }
    }

    private final long startTime;
    private final long finishBy;

    private final Map<String, List<CollaboratorParameter>> collaborationNeedsByName = Maps.newHashMap();
    private final Map<Class<?>, List<CollaboratorParameter>> collaborationNeedsByType = Maps.newHashMap();
    //  TODO: will need to remove collaborations from this when the timeout-annotated dependency is satisfied
    private final SortedMap<Long, List<Collaboration>> collaborationsByTimeDue = Maps.newTreeMap();

    private final CountDownLatch incompleteCollaborations;

    private final Results results = new Results();

    private final Executor executor;

    public Cascade(final long startTime, final long finishBy, final Executor executor, final Collection<Object> allCollaborators, final String id) {
        this.startTime = startTime;
        this.finishBy = finishBy;
        this.executor = executor;
        this.incompleteCollaborations = new CountDownLatch(allCollaborators.size());
        this.setUpCollaborators(allCollaborators, startTime);
        publish(this, String.class, RESULT_ID, id);
        publish(this, Publisher.class, RESULT_PUBLISHER, this); //  needed if anything is going to publish
    }

    private synchronized void setUpCollaborators(final Collection<Object> allCollaborators, final long startTime) {
        for (final Object collaborator : allCollaborators) {
            final Method method = findCollaborationMethod(collaborator.getClass().getMethods());
            final Annotation[][] parameterAnnotations = method.getParameterAnnotations();

            final long deadline = determineCollaboratorDeadline(startTime, parameterAnnotations);
            final Collaboration collaboration = new Collaboration(collaborator, method, deadline);

            final Class<?>[] parameterTypes = method.getParameterTypes();

            for (int i = 0; i < parameterAnnotations.length; i++) {
                final CollaboratorParameter target = new CollaboratorParameter(collaboration, i);
                List<CollaboratorParameter> collaborationNeedsForType = collaborationNeedsByType.get(parameterTypes[i]);
                if (collaborationNeedsForType == null) {
                    collaborationNeedsForType = Lists.newArrayList();
                    collaborationNeedsByType.put(parameterTypes[i], collaborationNeedsForType);
                }
                collaborationNeedsForType.add(target);

                for (int j = 0; j < parameterAnnotations.length; j++) {
                    if (Input.class.isAssignableFrom(parameterAnnotations[i][j].annotationType())) {
                        final Input input = (Input) parameterAnnotations[i][j];

                        if (input.name() != null) { //  TODO: also !empty
                            List<CollaboratorParameter> collaborationNeedsForName = collaborationNeedsByName.get(input.name());
                            if (collaborationNeedsForName == null) {
                                collaborationNeedsForName = Lists.newArrayList();
                                collaborationNeedsByName.put(input.name(), collaborationNeedsForName);
                            }
                            collaborationNeedsForName.add(target);
                        }
                    }
                }
            }

            List<Collaboration> collaborationsAtTheSameTime = collaborationsByTimeDue.get(deadline);
            if (collaborationsAtTheSameTime == null) {
                collaborationsAtTheSameTime = Lists.newArrayList();
                collaborationsByTimeDue.put(deadline, collaborationsAtTheSameTime);
            }
            collaborationsAtTheSameTime.add(collaboration);
        }
    }

    private static long determineCollaboratorDeadline(final long startTime, final Annotation[][] parameterAnnotations) {
        long overallDeadline = startTime;
        for (int i = 0; i < parameterAnnotations.length; i++) {
            for (int j = 0; j < parameterAnnotations.length; j++) {
                if (Input.class.isAssignableFrom(parameterAnnotations[i][j].annotationType())) {
                    final Input input = (Input) parameterAnnotations[i][j];

                    final long parameterDeadline = (input.timeout() + startTime);
                    if (overallDeadline < parameterDeadline) {
                        overallDeadline = parameterDeadline;
                    }
                }
            }
        }
        return overallDeadline;
    }

    class Results {
        final Map<String, Object> resultsByName = Maps.newConcurrentMap();
        final Map<Class<?>, Object> resultsByType = Maps.newConcurrentMap();
    }

    /**
     * Returns results either when everything is complete or there's no more time
     * @return
     */
    public Results collect() {
        while (true) {
            final long wakeUp;
            final Long firstDue;
            {
                final long now = clock();
                synchronized (this) {
                    firstDue = collaborationsByTimeDue.firstKey();
                }
                if ((firstDue == null) || (firstDue > finishBy)) {
                    wakeUp = finishBy - now;
                } else {
                    wakeUp = firstDue - now;
                }
                if (wakeUp <= 0) {  //  if we can't take the time to wait, quit
                    break;
                }
            }
            try {
                final boolean allDone = incompleteCollaborations.await(wakeUp, TimeUnit.MILLISECONDS);
                if (allDone) {  //  if everything is done, great; we can quit
                    break;
                }
                final long now = clock();
                if (finishBy <= now) {  //  if we've run out of time to do anything, quit
                    break;
                }

                //  dispatch everything that is due or overdue
                final List<Collaboration> readyToGo;
                synchronized (this) {
                    if (collaborationsByTimeDue.isEmpty()) {
                        continue;
                    }
                    readyToGo = Lists.newArrayList();
                    for (final Iterator<Entry<Long, List<Collaboration>>> iterator = collaborationsByTimeDue.entrySet().iterator();
                            iterator.hasNext(); ) {
                        final Entry<Long, List<Collaboration>> entry = iterator.next();
                        if (entry.getKey() > now) {
                            break;
                        }
                        readyToGo.addAll(entry.getValue());
                        iterator.remove();
                    }
                }

                for (final Collaboration collaboration : readyToGo) {
                    invoke(collaboration);
                }
            } catch (InterruptedException e) {
                // TODO
            }
        }
        return results;
    }


    public void publish(final Object collaborator, final Class<?> outputClass, final String name, final Object output) {
        //  TODO: log that this was produced by collaborator
 
        final List<CollaboratorParameter> collaborationTargets;
        synchronized (this) {
            if (name == null) {
                collaborationTargets = collaborationNeedsByType.remove(outputClass);
            } else {
                collaborationTargets = collaborationNeedsByName.remove(name);
            }
        }
        //  outside the synchronized block
        if (name == null) {
            results.resultsByType.put(outputClass, output); //  TODO: null
        } else {
            results.resultsByName.put(name, output);    //  TODO: null
        }

        incompleteCollaborations.countDown();

        if (collaborationTargets == null) {
            return;
        }

        for (final Iterator<CollaboratorParameter> iterator = collaborationTargets.iterator(); iterator.hasNext();) {
            final CollaboratorParameter target = iterator.next();
            final Collaboration collaboration = target.getCollaboration();
            collaboration.resolveParameter(target.getParameterIndex(), output);
            if (collaboration.allResolved()) {
                invoke(collaboration);
            }
        }
    }

    private void invoke(final Collaboration collaboration) {
        //  TODO: request regulator
        final Object collaborator = collaboration.getCollaborator();
        final Method foundMethod = collaboration.getMethod();
        final Object[] parameters = collaboration.getParameters();

        final Output outputAnnotation = foundMethod.getAnnotation(Output.class);
        final long now = clock();

        if ((finishBy > now) || ((outputAnnotation != null) && outputAnnotation.executeAfterTimeout())) {
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        // TODO: log call
                        final Object result = foundMethod.invoke(collaborator, parameters);
                        // TODO: log result (redundant with publishing)
                        String name = null;
                        final Output outputAnnotation = foundMethod.getAnnotation(Output.class);
                        if (outputAnnotation != null) {
                            name = outputAnnotation.name();
                        }
                        publish(collaborator, foundMethod.getReturnType(), name, result);
                    } catch (final IllegalAccessException e) {
                        LOGGER.error(methodErrorString(foundMethod, collaborator, parameters), e);
                    } catch (final IllegalArgumentException e) {
                        LOGGER.error(methodErrorString(foundMethod, collaborator, parameters), e);
                    } catch (final InvocationTargetException e) {
                        LOGGER.error(methodErrorString(foundMethod, collaborator, parameters), e);
                    }
                }
            });
        } else {
            //  TODO: log abort
        }
    }


    private static String methodErrorString(Method foundMethod, Object collaborator, Object[] arguments) {
        throw new RuntimeException("Implement me!");
    }

    //  TODO: make overridable
    private long clock() {
        return System.currentTimeMillis();
    }

    private static Method findCollaborationMethod(final Method[] methods) {
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
        return found;
    }
}
