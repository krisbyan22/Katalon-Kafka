import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject
import com.kms.katalon.core.checkpoint.Checkpoint as Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling as FailureHandling
import com.kms.katalon.core.testcase.TestCase as TestCase
import com.kms.katalon.core.testdata.TestData as TestData
import com.kms.katalon.core.testng.keyword.TestNGBuiltinKeywords as TestNGKW
import com.kms.katalon.core.testobject.TestObject as TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows
import internal.GlobalVariable as GlobalVariable
import org.openqa.selenium.Keys as Keys

import kafkaUtils.KafkaProducerKeyword
import kafkaUtils.KafkaConsumerKeyword
import com.kms.katalon.core.util.KeywordUtil

// Step 1 - Kirim multiple pesan
KafkaProducerKeyword producer = new KafkaProducerKeyword()
List<String> messagesToSend = []

for (int i = 1; i <= 5; i++) {
	String msg = "Pesan ke-" + i + " dari Katalon jam " + System.currentTimeMillis()
	messagesToSend.add(msg)
	producer.sendMessage("test-topic", msg)
	KeywordUtil.logInfo("Pesan dikirim: " + msg)
}

// Step 2 - Baca kembali dari Kafka
KafkaConsumerKeyword consumer = new KafkaConsumerKeyword()
def receivedMessages = consumer.readMessages("test-topic")

KeywordUtil.logInfo("Pesan diterima dari Kafka:")
receivedMessages.each { KeywordUtil.logInfo(it) }

// Step 3 - Verifikasi semua pesan berhasil diterima
int foundCount = 0
messagesToSend.each { originalMsg ->
	if (receivedMessages.contains(originalMsg)) {
		foundCount++
	} else {
		KeywordUtil.logWarning("Pesan tidak ditemukan: " + originalMsg)
	}
}

assert foundCount == messagesToSend.size() : "Tidak semua pesan diterima! (${foundCount}/${messagesToSend.size()})"
