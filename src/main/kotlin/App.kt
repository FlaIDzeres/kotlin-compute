import java.io.*
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.experimental.intrinsics.suspendCoroutineOrReturn
import kotlin.coroutines.experimental.startCoroutine

/**
 * Created by flaidzeres on 30.04.17.
 */

private val kotlinSerPath = "files/kotlin/"

fun main(args: Array<String>) {

    val dir = File(kotlinSerPath)

    if (!dir.exists()) dir.mkdirs()

    run()
}

fun run() = compute {
    val val0 = 0

    to("nodeId1")

    val val1 = val0 + 1

    println("Compute on nodeId1 val1=$val1")

    to("nodeId2")

    val val2 = val1 + 2

    println("Compute on nodeId2 val1=$val1 val2=$val2")

    to("nodeId3")

    val val3 = val2 + 3

    println("Compute on nodeId3 val1=$val1 val2=$val2 val3=$val3")

    println("Compute result:" + val3)
}

fun compute(block: suspend () -> Unit) {
    block.startCoroutine(object : Continuation<Unit>, Serializable {
        override val context: CoroutineContext
            get() = object : CoroutineContext, Serializable {
                override fun <E : CoroutineContext.Element> get(key: CoroutineContext.Key<E>): E? = null
                override fun <R> fold(initial: R, operation: (R, CoroutineContext.Element) -> R): R = initial
                override fun plus(context: CoroutineContext): CoroutineContext = context
                override fun minusKey(key: CoroutineContext.Key<*>): CoroutineContext = this
                override fun hashCode(): Int = 0
                override fun toString(): String = "EmptyCoroutineContext"
            }

        override fun resume(value: Unit) {
            println("Complete coroutine")
        }

        override fun resumeWithException(exception: Throwable) {
            println("Complete coroutine with " + exception)
        }
    })
}

suspend fun to(nodeId: String) {
    suspendCoroutineOrReturn<Unit> { con ->
        try {
            println("Serialize callback")

            val out = ObjectOutputStream(FileOutputStream(kotlinSerPath + "callBack" + nodeId))

            out.writeObject(con)

            out.close()

            // Send to another node
            println("Send compute to " + nodeId)

            // Received callback on {nodeId}
            println("DeSerialize callback")

            val `in` = ObjectInputStream(FileInputStream(kotlinSerPath + "callBack" + nodeId))

            val run0 = `in`.readObject() as Continuation<Unit>

            `in`.close()

            run0.resume(Unit)
        } catch (e: Throwable) {
            println("Got exception:" + e)
        }

        COROUTINE_SUSPENDED
    }
}