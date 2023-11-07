package com.hazelcast.persistence;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapdata.entity.schema.value.DateTime;
import org.bson.*;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static java.lang.String.format;

/**
 * @author samuel
 * @Description
 * @create 2022-11-16 15:48
 **/
public class MongodbUtil {

	public static MongoClient createClient(String uri, MongoClientSettings settings) {
		if (null == uri || "".equals(uri)) {
			throw new IllegalArgumentException("MongoDB uri cannot be blank");
		}
		MongoClientSettings.Builder settingBuilder;
		if (null != settings) {
			settingBuilder = MongoClientSettings.builder(settings);
		} else {
			settingBuilder = MongoClientSettings.builder();
		}
		settingBuilder.applyConnectionString(new ConnectionString(uri))
				.codecRegistry(getForJavaCodecRegistry());
		return MongoClients.create(settingBuilder.build());
	}

	private static CodecRegistry customCodecRegistry(List<Codec<?>> codecs, Map<BsonType, Class<?>> replacementsForDefaults) {

		BsonTypeClassMap bsonTypeCodecMap = new BsonTypeClassMap(replacementsForDefaults);
		DocumentCodecProvider documentCodecProvider = new DocumentCodecProvider(bsonTypeCodecMap);

		CodecRegistry defaultCodecRegistry = MongoClientSettings.getDefaultCodecRegistry();

		return CodecRegistries.fromRegistries(
				CodecRegistries.fromCodecs(codecs),
				CodecRegistries.fromProviders(documentCodecProvider),
				defaultCodecRegistry
		);
	}

	public static CodecRegistry getForJavaCodecRegistry() {
		Map<BsonType, Class<?>> replacements = new HashMap<>();
		replacements.put(BsonType.DECIMAL128, BigDecimal.class);
		replacements.put(BsonType.BINARY, byte[].class);
		replacements.put(BsonType.DATE_TIME, Date.class);
		replacements.put(BsonType.JAVASCRIPT, String.class);
		replacements.put(BsonType.JAVASCRIPT_WITH_SCOPE, String.class);
		replacements.put(BsonType.STRING, String.class);
		replacements.put(BsonType.SYMBOL, String.class);
		replacements.put(BsonType.TIMESTAMP, Date.class);

		return MongodbUtil.customCodecRegistry(
				Arrays.asList(
						new BigIntegerCodec(), new BigDecimalCodec(), new FloatCodec(), new ByteArrayCodec(),
						new DateCodec(), new StringCodec(), new DateTimeCodec()
				),
				replacements
		);
	}

	private static class BigIntegerCodec implements Codec<BigInteger> {

		@Override
		public BigInteger decode(BsonReader reader, DecoderContext decoderContext) {
			return new BigInteger(reader.readString());
		}

		@Override
		public void encode(BsonWriter writer, BigInteger value, EncoderContext encoderContext) {
			writer.writeString(value.toString());
		}

		@Override
		public Class<BigInteger> getEncoderClass() {
			return BigInteger.class;
		}
	}

	private static class BigDecimalCodec implements Codec<BigDecimal> {

		@Override
		public BigDecimal decode(BsonReader reader, DecoderContext decoderContext) {
			return reader.readDecimal128().bigDecimalValue();
		}

		@Override
		public void encode(BsonWriter writer, BigDecimal value, EncoderContext encoderContext) {
			writer.writeDecimal128(new Decimal128(value));
		}

		@Override
		public Class<BigDecimal> getEncoderClass() {
			return BigDecimal.class;
		}
	}

	private static class FloatCodec implements Codec<Float> {

		@Override
		public void encode(final BsonWriter writer, final Float value, final EncoderContext encoderContext) {
			if (value != null) {
				writer.writeDouble(new BigDecimal(value.toString()).doubleValue());
			} else {
				writer.writeNull();
			}
		}

		@Override
		public Float decode(final BsonReader reader, final DecoderContext decoderContext) {
			double value = decodeDouble(reader);
			if (value < -Float.MAX_VALUE || value > Float.MAX_VALUE) {
				throw new BsonInvalidOperationException(format("%s can not be converted into a Float.", value));
			}
			return (float) value;
		}

		private double decodeDouble(final BsonReader reader) {
			double doubleValue;
			BsonType bsonType = reader.getCurrentBsonType();
			switch (bsonType) {
				case INT32:
					doubleValue = reader.readInt32();
					break;
				case INT64:
					long longValue = reader.readInt64();
					doubleValue = longValue;
					if (longValue != (long) doubleValue) {
						throw invalidConversion(Double.class, longValue);
					}
					break;
				case DOUBLE:
					doubleValue = reader.readDouble();
					break;
				default:
					throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
			}
			return doubleValue;
		}

		private static <T extends Number> BsonInvalidOperationException invalidConversion(final Class<T> clazz, final Number value) {
			return new BsonInvalidOperationException(format("Could not convert `%s` to a %s without losing precision", value, clazz));
		}

		@Override
		public Class<Float> getEncoderClass() {
			return Float.class;
		}
	}

	private static class ByteArrayCodec implements Codec<byte[]> {

		@Override
		public byte[] decode(BsonReader reader, DecoderContext decoderContext) {
			BsonBinary bsonBinary = reader.readBinaryData();
			return bsonBinary.getData();
		}

		@Override
		public void encode(BsonWriter writer, byte[] value, EncoderContext encoderContext) {
			writer.writeBinaryData(new BsonBinary(value));
		}

		@Override
		public Class<byte[]> getEncoderClass() {
			return byte[].class;
		}
	}

	private static class DateCodec implements Codec<Date> {
		@Override
		public void encode(final BsonWriter writer, final Date value, final EncoderContext encoderContext) {
			writer.writeDateTime(value.getTime());
		}

		@Override
		public Date decode(final BsonReader reader, final DecoderContext decoderContext) {
			BsonType currentBsonType = reader.getCurrentBsonType();
			if (currentBsonType == BsonType.TIMESTAMP) {
				return new Date((long) reader.readTimestamp().getTime() * 1000);
			}
			return new Date(reader.readDateTime());
		}

		@Override
		public Class<Date> getEncoderClass() {
			return Date.class;
		}
	}

	private static class StringCodec implements Codec<String> {
		@Override
		public void encode(final BsonWriter writer, final String value, final EncoderContext encoderContext) {
			writer.writeString(value);
		}

		@Override
		public String decode(final BsonReader reader, final DecoderContext decoderContext) {
			BsonType currentBsonType = reader.getCurrentBsonType();
			switch (currentBsonType) {
				case SYMBOL:
					return reader.readSymbol();
				case OBJECT_ID:
					return reader.readObjectId().toHexString();
				case JAVASCRIPT:
					return reader.readJavaScript();
				case JAVASCRIPT_WITH_SCOPE:
					return reader.readJavaScriptWithScope();
				default:
					return reader.readString();
			}
		}

		@Override
		public Class<String> getEncoderClass() {
			return String.class;
		}

	}

	private static class DateTimeCodec implements Codec<DateTime> {
		@Override
		public DateTime decode(BsonReader bsonReader, DecoderContext decoderContext) {
			return new DateTime(bsonReader.readDateTime());
		}

		@Override
		public void encode(BsonWriter bsonWriter, DateTime dateTime, EncoderContext encoderContext) {
			if (null == dateTime) {
				bsonWriter.writeNull();
			} else {
				bsonWriter.writeDateTime(dateTime.toLong());
			}
		}

		@Override
		public Class<DateTime> getEncoderClass() {
			return DateTime.class;
		}
	}
}
