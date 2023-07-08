package org.bushwald.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import io.vertx.core.*
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import org.bushwald.vertx.sqs.impl.SqsClientImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.properties.Delegates

class SqsQueueConsumerVerticle() : AbstractVerticle(), SqsVerticle {

    override val log: Logger = LoggerFactory.getLogger(SqsQueueConsumerVerticle::class.java)

    constructor(credentialsProvider: AWSCredentialsProvider) : this() {
        this.credentialsProvider = credentialsProvider
    }
    override var credentialsProvider: AWSCredentialsProvider? = null

    override var client: SqsClient by Delegates.notNull()

    private var timerId: Long = -1

    override fun start(startFuture: Promise<Void>) {
        client = SqsClientImpl(vertx, config(), credentialsProvider)

        val queueUrl    = config().getString("queueUrl")
        val address     = config().getString("address")
        val maxMessages = config().getInteger("messagesPerPoll") ?: 1
        val timeout     = config().getLong("timeout") ?: SqsVerticle.DefaultTimeout

        val pollingInterval = config().getLong("pollingInterval")
        val awaitReplyBeforePolling = config().getBoolean("awaitReplyBeforePolling") ?: false

        client.start {
            if (it.succeeded()) {
                log.info ("started consumer verticle")
                if (awaitReplyBeforePolling) {
                    subscribeAwaitReply(pollingInterval, queueUrl, address, maxMessages, timeout)
                } else {
                    subscribe(pollingInterval, queueUrl, address, maxMessages, timeout)
                }
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    private fun subscribe(pollingInterval: Long, queueUrl: String, address: String, maxMessages: Int, timeout: Long) {
        timerId = vertx.setPeriodic(pollingInterval) {
            client.receiveMessages(queueUrl, maxMessages) {
                if (it.succeeded()) {
                    log.debug("Polled ${it.result().size} messages")
                    it.result().forEach { message ->
                        val reciept = message.getString("receiptHandle")

                        vertx.eventBus().request(address, message, DeliveryOptions().setSendTimeout(timeout)) { ar: AsyncResult<Message<Void>> ->
                            if (ar.succeeded()) {
                                // Had to code it like this, as otherwise I was getting 'bad enclosing class' from Java compiler
                                deleteMessage(queueUrl, reciept)
                            } else {
                                log.warn("Message with receipt $reciept was failed to process by the consumer")
                            }
                        }
                    }
                } else {
                    log.error("Unable to poll messages from $queueUrl", it.cause())
                }
            }
        }
    }

    private fun subscribeAwaitReply(pollingInterval: Long, queueUrl: String, address: String, maxMessages: Int, timeout: Long) {
        val startTime = System.currentTimeMillis()
        client.receiveMessages(queueUrl, maxMessages) {
            if (it.succeeded()) {
                log.debug("Polled ${it.result().size} messages")
                if (it.result().size == 0) {
                    reschedule(startTime, pollingInterval, queueUrl, address, maxMessages, timeout)
                } else {
                    val futures = it.result().map { message ->
                        val receipt = message.getString("receiptHandle")
                        val promise = Promise.promise<Message<Void>>()
                        vertx.eventBus().request(address, message, DeliveryOptions().setSendTimeout(timeout)) { messageReply: AsyncResult<Message<Void>> ->
                            if (messageReply.succeeded()) {
                                deleteMessage(queueUrl, receipt)
                            } else {
                                log.warn("Message with receipt $receipt was failed to process by the consumer")
                            }
                            promise.complete()
                        }
                        promise.future()
                    }

                    Future.all(futures).onComplete { _ ->
                        // All messages have been processed, reschedule
                        reschedule(startTime, pollingInterval, queueUrl, address, maxMessages, timeout)
                    }
                }
            } else {
                log.error("Unable to poll messages from $queueUrl", it.cause())
                // Reschedule in case of error
                reschedule(startTime, pollingInterval, queueUrl, address, maxMessages, timeout)
            }
        }
    }

    private fun reschedule(startTime: Long, pollingInterval: Long, queueUrl: String, address: String, maxMessages: Int, timeout: Long) {
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime
        val delay = if ( elapsedTime > pollingInterval) 1 else pollingInterval - elapsedTime
        timerId = vertx.setTimer(delay) {
            subscribeAwaitReply(pollingInterval, queueUrl, address, maxMessages, timeout)
        }
    }


    override fun stop(stopFuture: Promise<Void>) {
        vertx.cancelTimer(timerId)
        client.stop {
            if (it.succeeded()) {
                stopFuture.complete()
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }
}