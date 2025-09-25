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

// Step 1 - Kirim pesan
KafkaProducerKeyword producer = new KafkaProducerKeyword()
String testMessage = "Pesan test dari Katalon jam " + System.currentTimeMillis()
producer.sendMessage("test-topic", testMessage)

KeywordUtil.logInfo("Pesan dikirim: " + testMessage)

// Step 2 - Baca kembali
KafkaConsumerKeyword consumer = new KafkaConsumerKeyword()
def messages = consumer.readMessages("test-topic")

KeywordUtil.logInfo("Pesan diterima dari Kafka:")
messages.each { KeywordUtil.logInfo(it) }

int expectedMinCount = 1
assert messages.size() >= expectedMinCount : "Jumlah pesan kurang dari ekspektasi! (expected ≥ ${expectedMinCount}, actual = ${messages.size()})"


// Step 3 - Verifikasi pesan terkirim ditemukan
boolean found = messages.any { it == testMessage }
assert found : "Pesan yang dikirim tidak ditemukan!"
