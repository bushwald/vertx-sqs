package org.bushwald.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import org.slf4j.Logger


interface SqsVerticle {

    companion object {
        val DefaultTimeout = 5 * 60 * 1000L // 5 minutes
    }

    val log: Logger
    val client: SqsClient
    var credentialsProvider: AWSCredentialsProvider?

}

fun SqsVerticle.deleteMessage(queueUrl: String, reciept: String) {
    client.deleteMessage(queueUrl, reciept) {
        if (it.failed()) {
            log.warn("Unable to acknowledge message deletion with receipt = $reciept")
        }
    }
}