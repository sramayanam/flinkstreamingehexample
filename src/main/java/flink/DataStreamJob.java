package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;
public class DataStreamJob {

	public static void main(String[] args) {

			try {
				final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				final String TOPIC = "click_events";
				GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
				DataGeneratorSource<String> generatorSource =
						new DataGeneratorSource<>(
								generatorFunction,
								Long.MAX_VALUE,
								RateLimiterStrategy.perSecond(8),
								Types.STRING);

				DataStreamSource<String> streamSource =
						env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

				//streamSource.print();

				InputStream input = DataStreamJob.class.getClassLoader().getResourceAsStream("producer.properties");

				// load properties from file
				Properties properties = new Properties();
				properties.load(input);

				// set properties.
				KafkaSinkBuilder<String> kafkaSinkBuilder = KafkaSink.builder();
				for(String property: properties.stringPropertyNames())
				{
					kafkaSinkBuilder.setProperty(property, properties.getProperty(property));
				}

				// set serializer type.
				kafkaSinkBuilder.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(TOPIC)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build());

				KafkaSink<String> kafkaSink = kafkaSinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

				streamSource.sinkTo(kafkaSink);
				env.execute("Sinking Data to EH");

			} catch(FileNotFoundException e){
				System.out.println("FileNotFoundException: " + e);
			} catch (Exception e) {
				System.out.println("Failed with exception:: " + e);
			}
	}

}

