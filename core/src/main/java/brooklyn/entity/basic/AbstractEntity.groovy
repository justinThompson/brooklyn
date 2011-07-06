package brooklyn.entity.basic

import brooklyn.policy.Policy;

import java.lang.reflect.Field
import java.util.Collection
import java.util.Map
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.Application
import brooklyn.entity.Effector
import brooklyn.entity.Entity
import brooklyn.entity.EntityClass
import brooklyn.entity.Group
import brooklyn.entity.ParameterType
import brooklyn.event.AttributeSensor
import brooklyn.event.EventListener
import brooklyn.event.Sensor
import brooklyn.event.basic.AttributeMap
import brooklyn.event.basic.ConfigKey
import brooklyn.location.Location
import brooklyn.management.ExecutionContext
import brooklyn.management.ManagementContext
import brooklyn.management.SubscriptionContext
import brooklyn.management.SubscriptionHandle
import brooklyn.management.Task
import brooklyn.management.internal.BasicSubscriptionContext
import brooklyn.management.internal.AbstractManagementContext
import brooklyn.util.internal.LanguageUtils
import brooklyn.util.task.BasicExecutionContext

/**
 * Default {@link Entity} implementation.
 * 
 * Provides several common fields ({@link #name}, {@link #id});
 * a map {@link #config} which contains arbitrary config data;
 * sensors and effectors; policies; managementContext.
 * <p>
 * Fields in config can be accessed (get and set) without referring to config,
 * (through use of propertyMissing). Note that config is typically inherited
 * by children, whereas the fields are not. (Attributes cannot be so accessed,
 * nor are they inherited.)
 *
 * @author alex, aled
 */
public abstract class AbstractEntity implements EntityLocal, GroovyInterceptable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntity.class)
 
    String id = LanguageUtils.newUid()
    Map<String,Object> presentationAttributes = [:]
    String displayName
    final Collection<Group> groups = new CopyOnWriteArrayList<Group>()
    volatile Application application
    Collection<Location> locations = []
    Entity owner
    Collection<Policy> policies = [] as CopyOnWriteArrayList
    
    // following two perhaps belong in entity class in a registry;
    // but that is an optimization, and possibly wrong if we have dynamic sensors/effectors
    // (added only to this instance), however if we did we'd need to reset/update entity class
    // on sensor/effector set change
    /** map of effectors on this entity by name, populated at constructor time */
    private Map<String,Effector> effectors = null
    /** map of sensors on this entity by name, populated at constructor time */
    private Map<String,Sensor> sensors = null
    
    private transient EntityClass entityClass = null
    protected transient ExecutionContext execution
    protected transient SubscriptionContext subscription
    
    final Collection<Entity> ownedChildren = new CopyOnWriteArraySet<Entity>();
 
    /**
     * The sensor-attribute values of this entity. Updating this map should be done
     * via getAttribute/setAttribute; it will automatically emit an attribute-change event.
     */
    protected final AttributeMap attributesInternal = new AttributeMap(this)
    
    /**
     * For temporary data, e.g. timestamps etc for calculating real attribute values, such as when
     * calculating averages over time etc.
     */
    protected final Map<String,Object> tempWorkings = [:]
    
    /*
     * TODO An alternative implementation approach would be to have:
     *   setOwner(Entity o, Map<ConfigKey,Object> inheritedConfig=[:])
     * The idea is that the owner could in theory decide explicitly what in its config
     * would be shared.
     * I (Aled) am undecided as to whether that would be better...
     */
    /**
     * Map of configuration information that is defined at start-up time for the entity. These
     * configuration parameters are shared and made accessible to the "owned children" of this
     * entity.
     */
    protected final Map<ConfigKey,Object> ownConfig = [:]
    protected final Map<ConfigKey,Object> inheritedConfig = [:]

    
    public AbstractEntity(Entity owner) {
        this([:], owner)
    }
    
    public AbstractEntity(Map flags=[:], Entity owner=null) {
        this.@skipCustomInvokeMethod.set(true)
        try {
            if (flags.owner != null && owner != null && flags.owner != owner) {
                throw new IllegalArgumentException("Multiple owners supplied, ${flags.owner} and $owner")
            }
            Entity suppliedOwner = flags.remove('owner') ?: owner
            Map<ConfigKey,Object> suppliedOwnConfig = flags.remove('config')
        
            if (suppliedOwnConfig) ownConfig.putAll(suppliedOwnConfig)
        
            // initialize the effectors defined on the class
            // (dynamic effectors could still be added; see #getEffectors
            Map<String,Effector> effectorsT = [:]
            for (Field f in getClass().getFields()) {
                if (Effector.class.isAssignableFrom(f.getType())) {
                    Effector eff = f.get(this)
                    def overwritten = effectorsT.put(eff.name, eff)
                    if (overwritten!=null) LOG.warn("multiple definitions for effector ${eff.name} on $this; preferring $eff to $overwritten")
                }
            }
            LOG.trace "Entity {} effectors: {}", id, effectorsT.keySet().join(", ")
            effectors = effectorsT
    
            Map<String,Sensor> sensorsT = [:]
            for (Field f in getClass().getFields()) {
                if (Sensor.class.isAssignableFrom(f.getType())) {
                    Sensor sens = f.get(this)
                    def overwritten = sensorsT.put(sens.name, sens)
                    if (overwritten!=null) LOG.warn("multiple definitions for sensor ${sens.name} on $this; preferring $sens to $overwritten")
                }
            }
            LOG.trace "Entity {} sensors: {}", id, sensorsT.keySet().join(", ")
            sensors = sensorsT
    
            //set the owner if supplied; accept as argument or field
            if (suppliedOwner) suppliedOwner.addOwnedChild(this)
        } finally { this.@skipCustomInvokeMethod.set(false) }
    }

    /**
     * Adds this as a member of the given group, registers with application if necessary
     */
    public synchronized void setOwner(Entity entity) {
        if (owner != null) {
            if (owner == entity) return
            if (owner != entity) throw new UnsupportedOperationException("Cannot change owner of $this from $owner to $entity (owner change not supported)")
        }
        //make sure there is no loop
        if (this.equals(entity)) throw new IllegalStateException("entity $this cannot own itself")
        if (isDescendant(entity)) throw new IllegalStateException("loop detected trying to set owner of $this as $entity, which is already a descendent")
        
        owner = entity
        inheritedConfig.putAll(owner.getAllConfig())
        
        entity.addOwnedChild(this)
    }

    public boolean isAncestor(Entity oldee) {
        AbstractEntity ancestor = getOwner()
        while (ancestor) {
            if (ancestor.equals(oldee)) return true
            ancestor = ancestor.getOwner()
        }
        return false
    }

    public boolean isDescendant(Entity youngster) {
        Set<Entity> inspected = [] as HashSet
        List<Entity> toinspect = [this]
        
        while (!toinspect.isEmpty()) {
            Entity e = toinspect.pop()
            if (e.getOwnedChildren().contains(youngster)) {
                return true
            }
            inspected.add(e)
            toinspect.addAll(e.getOwnedChildren())
            toinspect.removeAll(inspected)
        }
        
        return false
    }

    /**
     * Adds the given entity as a member of this group <em>and</em> this group as one of the groups of the child;
     * returns argument passed in, for convenience.
     */
    @Override
    public Entity addOwnedChild(Entity child) {
        if (isAncestor(child)) throw new IllegalStateException("loop detected trying to add child $child to $this; it is already an ancestor")
        child.setOwner(this)
        ownedChildren.add(child)
        child
    }
 
    @Override
    public boolean removeOwnedChild(Entity child) {
        ownedChildren.remove child
        child.setOwner(null)
    }
    
    /**
     * Adds this as a member of the given group, registers with application if necessary
     */
    @Override
    public void addGroup(Group e) {
        groups.add e
        getApplication()
    }
 
    @Override
    public Entity getOwner() { owner }

    @Override
    public Collection<Group> getGroups() { groups }

    /**
     * Returns the application, looking it up if not yet known (registering if necessary)
     */
    @Override
    public Application getApplication() {
        if (this.@application!=null) return this.@application;
        def app = owner?.getApplication()
        if (app) {
            registerWithApplication(app)
            this.@application
        }
        app
    }

    @Override
    public String getApplicationId() {
        getApplication()?.id
    }

    @Override
    public ManagementContext getManagementContext() {
        getApplication()?.getManagementContext()
    }
    
    protected synchronized void registerWithApplication(Application app) {
        if (application) return;
        this.application = app
        app.registerEntity(this)
    }

    @Override
    public synchronized EntityClass getEntityClass() {
        if (!entityClass) {
            entityClass = new BasicEntityClass(getClass().getCanonicalName(), getSensors().values(), getEffectors().values())
        }

        return entityClass
    }

    @Override
    public Collection<Location> getLocations() {
        // TODO make result immutable, and use this.@locations when we want to update it?
        return locations;
    }

    /**
     * Should be invoked at end-of-life to clean up the item.
     */
    public void destroy() {
        //FIXME this doesn't exist, but we need some way of deleting stale items
        removeApplicationRegistrant()
    }

    @Override
    public <T> T getAttribute(AttributeSensor<T> attribute) {
        attributesInternal.getValue(attribute);
    }
    
    @Override
    public <T> T setAttribute(AttributeSensor<T> attribute, T val) {
        LOG.info "setting attribute {} to {}", attribute.name, val
        attributesInternal.update(attribute, val);
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        // FIXME What about inherited task in config?!
        Object v = ownConfig.get(key);
        v = v ?: inheritedConfig.get(key)

        //if config is set as a task, we wait for the task to complete
        while (v in Task) { v = v.get() }
        v
    }

    @Override
    public <T> T setConfig(ConfigKey<T> key, T val) {
        // TODO Is this the best idea, for making life easier for brooklyn coders when supporting changing config?
        if (application?.isDeployed()) throw new IllegalStateException("Cannot set configuration $key on active entity $this")
        
        T oldVal = ownConfig.put(key, val);
        if ((val in Task) && (!(val.isSubmitted()))) {
            //if config is set as a task, we make sure it starts running
            getExecutionContext().submit(val)
        }
        
        ownedChildren.each {
            it.refreshInheritedConfig()
        }
        
        oldVal
    }

    public void refreshInheritedConfig() {
        if (owner != null) {
            inheritedConfig.putAll(owner.getAllConfig())
        } else {
            inheritedConfig.clear();
        }
        
        ownedChildren.each {
            it.refreshInheritedConfig()
        }
    }
    
    @Override
    public Map<ConfigKey,Object> getAllConfig() {
        // FIXME What about task-based config?!
        Map<ConfigKey,Object> result = [:]
        result.putAll(ownConfig);
        result.putAll(inheritedConfig);
        return result.asImmutable()
    }

    /** @see Entity#subscribe(Entity, Sensor, EventListener) */
    public <T> SubscriptionHandle subscribe(Entity producer, Sensor<T> sensor, EventListener<T> listener) {
        subscriptionContext.subscribe(producer, sensor, listener)
    }
    
    /** @see Entity#subscribeToChildren(Entity, Sensor, EventListener) */
    public <T> SubscriptionHandle subscribeToChildren(Entity parent, Sensor<T> sensor, EventListener<T> listener) {
        subscriptionContext.subscribeToChildren(parent, sensor, listener)
    }

    protected synchronized SubscriptionContext getSubscriptionContext() {
        if (subscription) subscription;
        subscription = getManagementContext()?.getSubscriptionContext(this);
    }

    protected synchronized ExecutionContext getExecutionContext() {
        if (execution) execution;
        execution = new BasicExecutionContext(tag:this, getManagementContext().getExecutionManager())
    }
    
    /** default toString is simplified name of class, together with selected arguments */
    @Override
    public String toString() {
        StringBuffer result = []
        result << getClass().getSimpleName()
        if (!result) result << getClass().getName()
        result << "[" << toStringFieldsToInclude().collect({
            def v = this.hasProperty(it) ? this[it] : null  /* TODO would like to use attributes, config: this.properties[it] */
            v ? "$it=$v" : null
        }).findAll({it!=null}).join(",") << "]"
    }
 
    /** override this, adding to the collection, to supply fields whose value, if not null, should be included in the toString */
    public Collection<String> toStringFieldsToInclude() { ['id', 'displayName'] }

    
    // -------- POLICIES --------------------
    
    @Override
    public Collection<Policy> getPolicies() {
        return policies.asImmutable()
    }
    
    @Override
    public void addPolicy(Policy policy) {
        policies.add(policy)
        policy.setEntity(this)
    }

    @Override
    boolean removePolicy(Policy policy) {
        return policies.remove(policy)
    }
   

    // -------- SENSORS --------------------
    
    @Override
    public <T> void emit(Sensor<T> sensor, T val) {
        subscriptionContext?.publish(sensor.newEvent(this, val))
    }

    /**
     * Sensors available on this entity
     */
    public Map<String,Sensor<?>> getSensors() { sensors }
    
    /** convenience for finding named sensor in {@link #getSensor()} map */
    public <T> Sensor<T> getSensor(String sensorName) { getSensors()[sensorName] }
 
    /**
     * Add the given {@link Sensor} to this entity.
     */
    public void addSensor(Sensor<?> sensor) { sensors.put(sensor.name, sensor) }
 
    /**
     * Remove the named {@link Sensor} to this entity.
     */
    public void removeSensor(String sensorName) { sensors.remove(sensorName) }
    
    // -------- EFFECTORS --------------

    /** flag needed internally to prevent invokeMethod from recursing on itself */     
    private ThreadLocal<Boolean> skipCustomInvokeMethod = new ThreadLocal() { protected Object initialValue() { Boolean.FALSE } }

    public Object invokeMethod(String name, Object args) {
        if (!this.@skipCustomInvokeMethod.get()) {
            this.@skipCustomInvokeMethod.set(true);
            
            //args should be an array, warn if we got here wrongly (extra defensive as args accepts it, but it shouldn't happen here)
            if (args==null) LOG.warn("$this.$name invoked with incorrect args signature (null)", new Throwable("source of incorrect invocation of $this.$name"))
            else if (!args.getClass().isArray()) LOG.warn("$this.$name invoked with incorrect args signature (non-array ${args.getClass()}): "+args, new Throwable("source of incorrect invocation of $this.$name"))
            
            try {
                Effector eff = getEffectors().get(name)
                if (eff) {
                    args = AbstractEffector.prepareArgsForEffector(eff, args);
                    Task currentTask = executionContext.getCurrentTask();
                    if (!currentTask || !currentTask.getTags().contains(this)) {
                        //wrap in a task if we aren't already in a task that is tagged with this entity
                        MetaClass mc = metaClass
                        Task t = executionContext.submit( { mc.invokeMethod(this, name, args); },
                            description: "call to method $name being treated as call to effector $eff" )
                        return t.get();
                    }
                }
            } finally { this.@skipCustomInvokeMethod.set(false); }
        }
        metaClass.invokeMethod(this, name, args);
        //following is recommended on web site, but above is how groovy actually implements it
//            def metaMethod = metaClass.getMetaMethod(name, newArgs)
//            if (metaMethod==null)
//                throw new IllegalArgumentException("Invalid arguments (no method found) for method $name: "+newArgs);
//            metaMethod.invoke(this, newArgs)
    }
    
    /**
     * Effectors available on this entity.
     *
     * NB no work has been done supporting changing this after initialization,
     * but the idea of these so-called "dynamic effectors" has been discussed and it might be supported in future...
     */
    public Map<String,Effector> getEffectors() { effectors }
 
    /** convenience for finding named effector in {@link #getEffectors()} map */
    public <T> Effector<T> getEffector(String effectorName) { getEffectors()[effectorName] }
    
    public <T> Task<T> invoke(Map parameters=[:], Effector<T> eff) {
        invoke(eff, parameters);
    }
 
    //add'l form supplied for when map needs to be made explicit (above supports implicit named args)
    public <T> Task<T> invoke(Effector<T> eff, Map parameters) {
        executionContext.submit( { eff.call(this, parameters) }, description: "invocation of effector $eff" )
    }
}
