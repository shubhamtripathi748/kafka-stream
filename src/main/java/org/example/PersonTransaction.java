package org.example;

import org.apache.kafka.common.serialization.Serdes;

public class PersonTransaction extends Serdes {
    private String name;
    private int amount;
    private String date;

    public PersonTransaction() {
    }

    private int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public PersonTransaction(String name, int amount , String date, int count) {
        this.name = name;
        this.amount=amount;
        this.date=date;
        this.count=count;

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
