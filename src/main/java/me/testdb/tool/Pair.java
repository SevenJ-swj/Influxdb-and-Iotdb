package me.testdb.tool;


public class Pair<E extends Object, F extends Object> {
    private E first;
    private F second;

    public Pair() {
    }

    public Pair<E,F> make_pair(E first,F second) {
        this.first = first;
        this.second = second;
        return this;
    }
    public E getFirst() {
        return first;
    }

    public void setFirst(E first) {
        this.first = first;
    }

    public F getSecond() {
        return second;
    }

    public void setSecond(F second) {
        this.second = second;
    }
}

