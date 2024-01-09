package com.github.jeroenr

import com.github.jeroenr.Currency.USD
import io.kotest.matchers.doubles.plusOrMinus
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

val Dispatchers.LOOM: CoroutineDispatcher
    get() = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()

enum class Currency {
    USD, EUR
}
class ExchangeRateService(val name: String, val latency: Long, val rates: Map<Currency, Double>) {
    suspend fun getExchangeRate(currency: Currency): Double {
        println("${Thread.currentThread()} Fetching from $name...")
        delay(latency)
        val result = rates.getOrDefault(currency, 0.0)
        println("${Thread.currentThread()} Successfully fetched from $name!")
        return result
    }

    suspend fun close() {
        println("${Thread.currentThread()} Closing $name...")
        delay(20)
    }
}
class StructuredConcurrencyTest {

    private val exchanges = listOf(
        ExchangeRateService("EXCH-1", 1000, mapOf(USD to 125.12)),
        ExchangeRateService("EXCH-2 (fastest)", 500, mapOf(USD to 126.2)),
        ExchangeRateService("EXCH-3 (slowest)", 1500, mapOf(USD to 124.12))
    )
    @Test
    fun `should get results concurrently`() {
        val elapsedTime = measureTimeMillis {
            runBlocking(Dispatchers.Default) {
                exchanges.forEach { exch ->
                    launch { exch.getExchangeRate(USD) }
                }

            } //<- at the end of runBlocking all Coroutines will have been joined 'automagically' due to structured concurrency,
        }
        // the total time should not exceed (+/-) the latency of the slowest service
        elapsedTime.toDouble() shouldBe 1500.toDouble().plusOrMinus(100.0)
    }
    @Test
    fun `should get results concurrently using virtual threads`() {
        val elapsedTime = measureTimeMillis {
            runBlocking(Dispatchers.LOOM) {
                exchanges.forEach { exch ->
                    launch { exch.getExchangeRate(USD) }
                }

            } //<- at the end of runBlocking all Coroutines will have been joined 'automagically' due to structured concurrency,
        }
        // the total time should not exceed (+/-) the latency of the slowest service
        elapsedTime.toDouble() shouldBe 1500.toDouble().plusOrMinus(100.0)
    }

    @Test
    fun `should get the result of the fastest exchange`() {
        val elapsedTime = measureTimeMillis {
            runBlocking(Dispatchers.Default) {
                val deferredList = exchanges.map { exch -> async {exch.getExchangeRate(USD)} }
                val winner = select {
                    deferredList.forEach { deferred -> deferred.onAwait { it }}
                }
                deferredList.forEach { it.cancel()}

                println("Got rate: $winner")
                winner shouldBe 126.2
            }
        }
        // the total time should exceed (+/-) the latency of the fastest service
        elapsedTime.toDouble() shouldBe 500.toDouble().plusOrMinus(100.0)
    }

    @Test
    fun `should cancel slow exchanges`() {
        val elapsedTime = measureTimeMillis {
            runBlocking(Dispatchers.Default) {
                val results = exchanges.map { exch ->
                    async {
                        val currentThread = Thread.currentThread()
                        try {
                            withTimeout(1100) {
                                exch.getExchangeRate(USD).also {
                                    println("$currentThread Task completed with result '$it'!")
                                }
                            }
                        } catch (e: CancellationException) {
                            println("$currentThread Task cancelled!")
                            null
                        }
                    }
                }.mapNotNull { it.await() }

                results.firstOrNull()?.also { println("Got rate: $it") }
                results shouldBe listOf(125.12, 126.2)
            }
        }
        // the total time should not exceed (+/-) the threshold
        elapsedTime.toDouble() shouldBe 1100.toDouble().plusOrMinus(100.0)
    }

    @Test
    fun `should cancel slow exchanges and cleanup resources`() {
        val coroutinesMs = measureTimeMillis {
            runBlocking(Dispatchers.Default) {
                val results = exchanges.map { ex ->
                    val currentThread = Thread.currentThread()
                    async {
                        try {
                            withTimeout(1100) {
                                ex.getExchangeRate(Currency.USD).also {
                                    println("$currentThread Task completed with result '$it'!")
                                }
                            }
                        } catch (e: CancellationException) {
                            println("$currentThread Task cancelled!")
                            null
                        } finally {
                            withContext(NonCancellable) {
                                ex.close()
                            }
                        }
                    }
                }.mapNotNull {  it.await() }

                results shouldBe listOf(125.12, 126.2)
            }
        }
        // the total time should not exceed (+/-) the threshold
        coroutinesMs.toDouble() shouldBe 1100.toDouble().plusOrMinus(100.0)
    }

}