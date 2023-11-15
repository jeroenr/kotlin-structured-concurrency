package com.github.jeroenr

import io.kotest.matchers.doubles.plusOrMinus
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import com.github.jeroenr.Currency.USD
import org.junit.jupiter.api.Test
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.system.measureTimeMillis

val Dispatchers.LOOM: CoroutineDispatcher
    get() = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()

public fun <T> runVirtual(context: CoroutineContext = EmptyCoroutineContext, block: suspend CoroutineScope.() -> T): T =
    runBlocking(context + Dispatchers.LOOM, block)

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
}
class StructuredConcurrencyTest {

    private val exchanges = listOf(
        ExchangeRateService("exchange 1", 1000, mapOf(USD to 125.12)),
        ExchangeRateService("exchange 2 (fastest)", 500, mapOf(USD to 126.2)),
        ExchangeRateService("exchange 3 (slowest)", 1500, mapOf(USD to 124.12))
    )

    @Test
    fun `should get the result of the fastest exchange`() {
        println()
        println("Race test")
        println()
        val coroutinesMs = measureTimeMillis {
            runVirtual {
                val deferredList = exchanges.map { bank -> async {bank.getExchangeRate(USD)} }
                val winner = select {
                    deferredList.forEach { deferred -> deferred.onAwait { it }}
                }
                deferredList.forEach { it.cancel()}

                println("Got rate: $winner")
                winner shouldBe 126.2
            }
        }
        //important: the total time should not be greater (+/-) than the latency of the fastest service
        coroutinesMs.toDouble() shouldBe 500.toDouble().plusOrMinus(400.0)
    }

    @Test
    fun `should cancel slow exchanges`() {
        println()
        println("Cancel slow exchanges test")
        println()
        val coroutinesMs = measureTimeMillis {
            runVirtual {
                val results = exchanges.map { ex ->
                    async {
                        val currentThread = Thread.currentThread()
                        try {
                            withTimeout(800) {
                                ex.getExchangeRate(USD).also {
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
                results shouldBe listOf(126.2)
            }
        }
        //important: the total time should not be greater (+/-) than the latency of the fastest service
        coroutinesMs.toDouble() shouldBe 500.toDouble().plusOrMinus(400.0)
    }

}