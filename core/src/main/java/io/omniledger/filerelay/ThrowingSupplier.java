package io.omniledger.filerelay;

@FunctionalInterface
public interface ThrowingSupplier<E extends Exception, R> {

    R get() throws E;

}
