package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    NL(0),  // no lock held
    IS(1),  // intention shared
    IX(2),  // intention exclusive
    S(3),   // shared
    SIX(4), // shared intention exclusive
    X(5);   // exclusive

    public final int index;

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     *
     * order of matrix: NL, IS, IX, S, SIX, X
     *                 NL
     *                 IS
     *                 IX
     *                 S
     *                 SIX
     *                 X
     */
    static final boolean[][] compatibleMatrix = {
            {true, true, true, true, true, true},
            {true, true, true, true, true, false},
            {true, true, true, false, false, false},
            {true, true, false, true, false, false},
            {true, true, false, false, false, false},
            {true, false, false, false, false, false},
    };

    LockType(int index) {
        this.index = index;
    }

    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return compatibleMatrix[a.index][b.index];
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     *
     * order of matrix
     *   NL, IS, IX, S, SIX, X
     * NL
     * IS
     * IX
     * S
     * SIX
     * X
     */
    public static final boolean[][] canBeParentMatrix = {
            {true, false, false, false, false, false},
            {true, true, false, true, false, false},
            {true, true, true, true, true, true},
            {true, false, false, false, false, false},
            {true, false, false, false, false, false},
            {true, false, false, false, false, false},
    };
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        return canBeParentMatrix[parentLockType.index][childLockType.index];
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     *
     * order of matrix
     *   NL, IS, IX, S, SIX, X
     * NL
     * IS
     * IX
     * S
     * SIX
     * X
     */
    public static final boolean[][] substituteMatrix = {
            {true, false, false, false, false, false},
            {true, true, false, false, false, false},
            {true, true, true, false, false, false},
            {true, true, false, true, false, false},
            {true, true, true, true, true, false},
            {true, true, true, true, true, true}
    };
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        return substituteMatrix[substitute.index][required.index];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

