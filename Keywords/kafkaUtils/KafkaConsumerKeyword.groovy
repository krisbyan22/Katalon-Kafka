package kafkaUtils

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject

import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.checkpoint.Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling
import com.kms.katalon.core.testcase.TestCase
import com.kms.katalon.core.testdata.TestData
import com.kms.katalon.core.testobject.TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows

import internal.GlobalVariable

import org.apache.kafka.clients.consumer.*
import com.kms.katalon.core.annotation.Keyword

class KafkaConsumerKeyword {

	@Keyword
	def readMessages(String topic) {
		Properties props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")
		props.put("group.id", "katalon-group")
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("auto.offset.reset", "earliest")

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
		consumer.subscribe(Collections.singletonList(topic))

		def messages = []
		ConsumerRecords<String, String> records = consumer.poll(3000) // 3 detik
		for (ConsumerRecord<String, String> record : records) {
			messages.add(record.value())
		}
		consumer.close()
		return messages
	}
	@Keyword
	def readMessagesWithGroup(String topic, String groupId) {
		Properties props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")
		props.put("group.id", groupId)
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("auto.offset.reset", "latest")  // bisa diganti "latest"

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
		consumer.subscribe(Collections.singletonList(topic))

		def messages = []
		ConsumerRecords<String, String> records = consumer.poll(3000)
		for (ConsumerRecord<String, String> record : records) {
			messages.add(record.value())
		}
		consumer.close()
		return messages
	}

	@Keyword
	def readAndLogOffset(String topic, String groupId, int delayMs = 0) {
		Properties props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")
		props.put("group.id", groupId)
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("auto.offset.reset", "earliest")
		props.put("enable.auto.commit", "true")

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
		consumer.subscribe(Collections.singletonList(topic))

		if (delayMs > 0) Thread.sleep(delayMs)

		def messages = []
		ConsumerRecords<String, String> records = consumer.poll(3000)
		for (ConsumerRecord<String, String> record : records) {
			messages.add(record.value())
			println "Consumed (offset: ${record.offset()}): ${record.value()}"
		}
		consumer.close()
		return messages
	}


}
