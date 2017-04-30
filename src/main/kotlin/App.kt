import java.io.*
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.startCoroutine
import kotlin.coroutines.experimental.suspendCoroutine

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
    val v0 = 0

    to("nodeId1")

    val v1 = v0 + 1

    println("Compute on nodeId1 " + v1)

    to("nodeId2")

    val v2 = v1 + 2

    println("Compute on nodeId2 " + v2)

    to("nodeId3")

    val v3 = v2 + 3

    println("Compute on nodeId3 " + v3)

    println("Compute result:" + v3)
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

        override fun resume(value: Unit) {}

        override fun resumeWithException(exception: Throwable) {}
    })
}

suspend fun to(nodeId: String) {
    suspendCoroutine<Unit> { con ->
        try {
            val field = con.javaClass.getDeclaredField("delegate")

            field.isAccessible = true

            val con1 = field.get(con)

            println("Serialize callback")

            val out = ObjectOutputStream(FileOutputStream(kotlinSerPath + "callBack" + nodeId))

            out.writeObject(con1)

            // Send to another node
            println("Send compute to " + nodeId)

            // Received callback on {nodeId}
            println("DeSerialize callback")

            val `in` = ObjectInputStream(FileInputStream(kotlinSerPath + "callBack" + nodeId))

            val run0 = `in`.readObject() as Continuation<Unit>

            run0.resume(Unit)
        } catch (e: Throwable) {
            println("Got exception:" + e)
        }
    }
}