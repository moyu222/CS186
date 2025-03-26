package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TransactionContext;
import sun.jvm.hotspot.utilities.UnsupportedPlatformException;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // check if the lockType is duplicate
        LockType heldType = getExplicitLockType(transaction);
        if (heldType == lockType) {
            throw new DuplicateLockRequestException("The lockType is already held by this xact");
        }
        // check if the context is readonly
        if (readonly) {
            throw new UnsupportedPlatformException("This lockContext is readonly.");
        }
        // check if the request is invalid
        boolean valid = true;
        LockContext parentContext = parentContext();
        if (parentContext != null) {
            LockType parentType = lockman.getLockType(transaction, parentContext.name);
            valid = LockType.canBeParentLock(parentType, lockType);
            if (hasSIXAncestor(transaction)) {
                if (lockType == LockType.S || lockType == LockType.IS) {
                    valid = false;
                }
            }
        }
        if (!valid) {
            throw new InvalidLockException("The request does not meet multigranularity constraints.");
        }
        lockman.acquire(transaction, name, lockType);
        this.updateNumChildLocks(transaction, 1);

    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // check if there is a lock in context
        LockType heldType = lockman.getLockType(transaction,name);
        if (heldType == LockType.NL) {
            throw new NoLockHeldException("no lock is held by transaction.");
        }
        // check if the context is readonly
        if (readonly) {
            throw new UnsupportedPlatformException("This lockContext is readonly.");
        }
        // check if request is valid
//        boolean valid = true;
//        List<LockContext> descendants = computeDescendants(transaction);
//        List<LockType> descendantTypes = new ArrayList<>();
//        for (LockContext descendant : descendants) {
//            descendantTypes.add(lockman.getLockType(transaction,descendant.name));
//        }
//        if (descendantTypes.contains(LockType.S) || descendantTypes.contains(LockType.X)) {
//            valid = false;
//        } else if (descendantTypes.contains(LockType.IS) && descendantTypes.contains(LockType.IX)) {
//            valid = false;
//        }
//        if (!valid) {
//            throw new InvalidLockException("The request does not meet multigranularity constraints.");
//        }
        // releasing locks in bottom-up order
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("Request invalid: releasing locks on children first");
        }
        lockman.release(transaction, name);
        this.updateNumChildLocks(transaction, -1);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // check if the context is readonly
        if (readonly) {
            throw new UnsupportedPlatformException("This lockContext is readonly.");
        }
        // check duplicate lock
        LockType heldType = lockman.getLockType(transaction, name);
        if (heldType == newLockType) {
            throw new DuplicateLockRequestException("duplicate lockType.");
        }
        // check no lock held
        if (heldType == LockType.NL) {
            throw new NoLockHeldException("Transaction has no lock.");
        }
        // check valid
        if (!LockType.substitutable(newLockType, heldType)) {
            throw new InvalidLockException("invalid lock");
        }
        // if promote to SIX, release all S and IS
        if (newLockType == LockType.SIX) {
// we can not use LockContext.release(), because we do not know the order of sisDesName, their NumChildren may not 0
//            for (ResourceName descName : sisDescNames) {
//                LockContext sisContext = fromResourceName(lockman, descName);
//                sisContext.release(transaction);
//            }
            List<ResourceName> releaseNames = sisDescendants(transaction);
            // update numChildLocks for all releaseName
            updateNumChildLocks(transaction, -1, releaseNames);
            releaseNames.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
        } else {
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        // check no lock held
        LockType heldType = lockman.getLockType(transaction, name);
        if (heldType == LockType.NL) {
            throw new NoLockHeldException("No lock held.");
        }
        // check readonly
        if (readonly) {
            throw new UnsupportedOperationException("Read only can not escalate.");
        }
        // avoid multiple changes
        if (heldType == LockType.S || heldType == LockType.X) {
            return;
        }
        // determine S or X
        LockType changeToType = LockType.S;
        List<LockContext> descendants = computeDescendants(transaction);
        List<ResourceName> releaseNames = new ArrayList<>();
        for (LockContext descendant : descendants) {
            releaseNames.add(descendant.name);
        }
        releaseNames.add(name);
        if (heldType == LockType.IX || heldType == LockType.SIX) {
            changeToType = LockType.X;
        }
        updateNumChildLocks(transaction, -1, releaseNames);
        // only make *one* mutating call to the lock manager
        lockman.acquireAndRelease(transaction, name, changeToType, releaseNames);
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        // SIX case
        if (hasSIXAncestor(transaction)) {
            return LockType.S;
        }
        // if there is an effective lockType S and X case
        LockContext currContext = this;
        while (true) {
            LockContext ancestorContext = currContext.parentContext();
            if (ancestorContext == null) {
                break;
            }
            LockType parentType = lockman.getLockType(transaction, ancestorContext.name);
            if (parentType == LockType.S || parentType == LockType.X) {
                return parentType;
            }

            currContext = ancestorContext;
        }
        // explicit lock
        return getExplicitLockType(transaction);
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext currContext = this;
        while (true) {
            LockContext ancestorContext = currContext.parentContext();
            if (ancestorContext == null) {
                return false;
            }
            LockType parentType = lockman.getLockType(transaction, ancestorContext.name);
            if (parentType == LockType.SIX) {
                return true;
            }
            currContext = ancestorContext;
        }
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<LockContext> descendants = computeDescendants(transaction);
        List<ResourceName> result = new ArrayList<>();
        for (LockContext desContext : descendants) {
            if (lockman.getLockType(transaction, desContext.name) == LockType.S ||
                    lockman.getLockType(transaction, desContext.name) == LockType.IS) {
                result.add(desContext.name);
            }
        }
        return result;
    }

    /**
     * helper method to get all the descendant of a LockContext
     */
    private List<LockContext> computeDescendants(TransactionContext transaction) {
        List<Lock> heldLocks = lockman.getLocks(transaction);
        List<LockContext> childContext = new ArrayList<>();
        for (Lock lock : heldLocks) {
            if (name.isDescendantOf(lock.name)) {
                childContext.add(fromResourceName(lockman,lock.name));
            }
        }
        return childContext;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    /** a helper function to update ancestor's numChildLocks after acquire or release */
    // update numChildLocks with given delta recursively for all ancestors
    private void updateNumChildLocks(TransactionContext transaction, int delta) {
        numChildLocks.put(
                transaction.getTransNum(),
                numChildLocks.getOrDefault(transaction.getTransNum(), 0) + delta
        );
        LockContext parent = parentContext();
        if (parent != null) {
            parent.updateNumChildLocks(transaction, delta);
        }
    }

    // update numChildLocks with given delta recursively for all ancestors of given resources
    // numChildLocks of resource itself won't be updated
    private void updateNumChildLocks(TransactionContext transaction, int delta, List<ResourceName> resources) {
        for (ResourceName resource : resources) {
            LockContext parent = LockContext.fromResourceName(lockman, resource).parentContext();
            if (parent != null) {
                parent.updateNumChildLocks(transaction, delta);
            }
        }
    }



    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

