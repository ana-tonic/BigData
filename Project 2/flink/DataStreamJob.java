package com.example.flink;

import com.datastax.driver.mapping.Mapper;
import com.example.flink.model.CarData;
import com.example.flink.model.MaxSpeed;
import com.example.flink.model.MinSpeed;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.util.Collector;


public class DataStreamJob {

	public static final int INTERVAL = 10;

	public static void main(String[] args) throws Exception {

		String inputTopic = "topic1";
		String server = "kafka:9092";
		int topN = 3;

		StreamConsumer(inputTopic, server, topN);
	}

	public static void StreamConsumer(String inputTopic, String server, int topN) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
		DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

		DataStream<CarData> carDataStream =  stringInputStream.map(new MapFunction<String, CarData>() {
			@Override
			public CarData map(String value) throws Exception {
				return new Gson().fromJson(value, CarData.class);
			}
		});
		KeyedStream<CarData, String> keyedStream = carDataStream.keyBy(car -> car.getLocationId());

		getStatistics(stringInputStream, MaxSpeed.class, "max");
		getStatistics(stringInputStream, MinSpeed.class, "min");
		getAvg(keyedStream);
		mostVisited(carDataStream, topN);
		mostPollutedAreas(keyedStream, topN);

		environment.execute();
	}

	public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
			String topic, String kafkaAddress) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaAddress);
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
				topic, new SimpleStringSchema(), props);

		return consumer;
	}

	// minimalna i maksimalna vrednost atributa
	public static <T extends CarData> void getStatistics(DataStream<String> inputStream, Class<T> outputClass,
														 String operator) throws Exception {
		DataStream<T> dataStream = inputStream.map(new MapFunction<String, T>() {
			@Override
			public T map(String value) throws Exception {
				return new Gson().fromJson(value, outputClass);
			}
		}).returns(TypeInformation.of(outputClass));

		KeyedStream<T, String> keyedStream = dataStream.keyBy(car -> car.getLocationId());

		SingleOutputStreamOperator<T> results = null;
		switch (operator) {
			case "min":
				results = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
						.min("vehicleSpeed");
				break;
			case "max":
				results = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
						.max("vehicleSpeed");
				break;
			default:
				System.out.println("Unknown operator");
				break;
		}

		CassandraSink.addSink(results)
				.setHost("cassandra")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();
	}



	// srednja vrednost atributa brzina i cekanje, plus broj vozila po traci
	public static void getAvg(KeyedStream<CarData, String> keyedStream) throws Exception {

		SingleOutputStreamOperator<Tuple4<String, Float, Float, Integer>> result =
				keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
				.aggregate(new AggregateFunction<CarData, Tuple4<String, Float, Float, Integer>, Tuple4<String, Float, Float, Integer>>() {

					@Override
					public Tuple4<String, Float, Float, Integer> createAccumulator() {
						return new Tuple4<String, Float, Float, Integer>("", 0.0F, 0.0F, 0);
					}

					@Override
					public Tuple4<String, Float, Float, Integer> add(CarData value, Tuple4<String, Float, Float, Integer> accumulator) {
						float sum_speed = accumulator.f1 + value.getVehicleSpeed();
						float sum_waiting = accumulator.f2 + value.getVehicleWaiting();
						int count = accumulator.f3 + 1;
						String location = value.getLocationId();
						return new Tuple4<String, Float, Float, Integer>(location, sum_speed, sum_waiting, count);
					}

					@Override
					public Tuple4<String, Float, Float, Integer> getResult(Tuple4<String, Float, Float, Integer> accumulator) {
						Float res_speed = accumulator.f1/accumulator.f3;
						Float res_waiting = accumulator.f2/accumulator.f3;
						return new Tuple4<String, Float, Float, Integer>(accumulator.f0, res_speed, res_waiting, accumulator.f3);
					}

					@Override
					public Tuple4<String, Float, Float, Integer> merge(Tuple4<String, Float, Float, Integer> a,
																Tuple4<String, Float, Float, Integer> b) {
						String location = a.f0;
						Float speed = a.f1 + b.f1;
						Float waiting = a.f2 + b.f2;
						Integer count = a.f3 + b.f3;
						return new Tuple4<String, Float, Float, Integer>(location, speed, waiting, count);
					}
				});

		CassandraSink
				.addSink(result)
				.setHost("cassandra")
				.setQuery("INSERT INTO example.avg_data (partition_key, lane, avg_speed, avg_waiting, count) VALUES (uuid(), ?, ?, ?, ?);")
				.build();
	}

	// najveca guzva
	public static void mostVisited(DataStream<CarData> carDataStream, int topN) throws Exception {

		DataStream<Tuple2<String, Integer>> counts = carDataStream
				.map(new MapFunction<CarData, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(CarData value) throws Exception {
						return new Tuple2<String, Integer>(value.getLocationId(), 1);
					}
				})
				.keyBy(car -> car.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
				.sum(1)
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple2<String, Integer>> iterable,
										Collector<Tuple2<String, Integer>> collector) throws Exception {
						PriorityQueue<Tuple2<String, Integer>> queue = new PriorityQueue<>(Comparator.comparingInt(o -> o.f1));
						for (Tuple2<String, Integer> t : iterable) {
							queue.offer(t);
							if (queue.size() > topN) {
								queue.poll();
							}
						}
						List<Tuple2<String, Integer>> topN = new ArrayList<>(queue);
						topN.sort(Comparator.comparingInt(o -> -o.f1));
						for (Tuple2<String, Integer> t : topN) {
							collector.collect(t);
						}
					}
				});

		CassandraSink
				.addSink(counts)
				.setHost("cassandra")
				.setQuery("INSERT INTO example.visits_data (partition_key, location_id, count) VALUES (uuid(), ?, ?);")
				.build();
	}

	// najzagadjenija oblast
	public static void mostPollutedAreas(KeyedStream<CarData, String> keyedStream, int topN) throws Exception {

		DataStream<Tuple2<String, Float>> areas = keyedStream
				.window(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
				.aggregate(new AggregateFunction<CarData, Tuple2<String, Float>, Tuple2<String, Float>>() {

					@Override
					public Tuple2<String, Float> createAccumulator() {
						return new Tuple2<String, Float>("", 0.0F);
					}

					@Override
					public Tuple2<String, Float> add(CarData value,
													 Tuple2<String, Float> accumulator) {
						return new Tuple2<String, Float>(value.getLocationId(), value.getVehicleCO2() +value.getVehicleCO() +
								value.getVehicleHC() + value.getVehiclePMx() + value.getVehicleNoise() + accumulator.f1);
					}

					@Override
					public Tuple2<String, Float> getResult(Tuple2<String, Float> accumulator) {
						return accumulator;
					}

					@Override
					public Tuple2<String, Float> merge(Tuple2<String, Float> a, Tuple2<String, Float> b) {
						return new Tuple2<String, Float>(a.f0, a.f1 + b.f1);
					}
				})
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(INTERVAL)))
				.process(new ProcessAllWindowFunction<Tuple2<String, Float>, Tuple2<String, Float>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple2<String, Float>> iterable,
										Collector<Tuple2<String, Float>> collector) throws Exception {
						PriorityQueue<Tuple2<String, Float>> queue = new PriorityQueue<>(Comparator.comparingDouble(o -> o.f1));
						for (Tuple2<String, Float> t : iterable) {
							queue.offer(t);
							if (queue.size() > topN) {
								queue.poll();
							}
						}
						List<Tuple2<String, Float>> topN = new ArrayList<>(queue);
						topN.sort(Comparator.comparingDouble(o -> -o.f1));
						for (Tuple2<String, Float> t : topN) {
							collector.collect(t);
						}
					}
				});

		CassandraSink
				.addSink(areas)
				.setHost("cassandra")
				.setQuery("INSERT INTO example.pollution_data (partition_key, location_id, pollution) VALUES (uuid(), ?, ?);")
				.build();
	}
}
