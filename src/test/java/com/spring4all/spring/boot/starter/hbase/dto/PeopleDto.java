package com.spring4all.spring.boot.starter.hbase.dto;


public class PeopleDto {

    private String name;

    private Integer age;

    public String getName() {
        return name;
    }

    public PeopleDto setName(String name) {
        this.name = name;
        return this;
    }

    public Integer getAge() {
        return age;
    }

    public PeopleDto setAge(Integer age) {
        this.age = age;
        return this;
    }

    @Override
    public String toString() {
        return "PeopleDto{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}