package com.example.flink.model;

import com.datastax.driver.mapping.annotations.Table;
@Table(keyspace = "example", name = "max_data")
public class MaxSpeed extends CarData {}
