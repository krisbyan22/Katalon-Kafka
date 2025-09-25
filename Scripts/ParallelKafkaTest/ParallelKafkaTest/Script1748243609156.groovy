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

import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import java.nio.file.Files
import java.nio.file.Paths

// === STEP 1: Jalankan Kafka consumer paralel (multithreaded)
println "[INFO] Starting parallel Kafka consumer..."
String consumerCommand = "java -cp libs/*:kafka-consumer.jar ParallelKafkaConsumer"
Runtime.getRuntime().exec(consumerCommand)

// === STEP 2: Trigger API yang kirim event ke Kafka
println "[INFO] Triggering API to send event..."
def response = WS.sendRequest(findTestObject('API/SendKafkaEvent'))
WS.verifyResponseStatusCode(response, 200)

// === STEP 3: Tunggu pemrosesan selesai
println "[INFO] Waiting for message to be consumed..."
Thread.sleep(5000)  // Delay 5 detik (atau gunakan polling log)

// === STEP 4: Validasi dari log Kafka consumer
println "[INFO] Validating message consumption from log..."
def logPath = "logs/kafka-consumer.log"
def logContent = new String(Files.readAllBytes(Paths.get(logPath)))

if (!logContent.contains("Received: event123")) {
	KeywordUtil.markFailed("Message not found in Kafka log!")
} else {
	println "[SUCCESS] Message found in log!"
}

// === STEP 5 (optional): Validasi DB (jika ada)
println "[INFO] Validating from database..."
def dbResult = CustomKeywords.'utils.DatabaseHelper.query'('SELECT status FROM kafka_log WHERE id="event123"')
assert dbResult == "PROCESSED"
