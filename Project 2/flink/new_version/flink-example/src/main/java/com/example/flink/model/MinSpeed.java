package com.example.flink.model;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "example", name = "min_data")
public class MinSpeed extends CarData {}

