import java.util.concurrent.atomic.*
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class BlockingStackImpl<E> : BlockingStack<E> {

    // ==========================
    // Segment Queue Synchronizer
    // ==========================

    private val dummy = Receiver<E>(null)
    private val enqIdx = AtomicReference<Receiver<E>>(dummy)
    private val deqIdx = AtomicReference<Receiver<E>>(dummy)

    private data class Receiver<E>(
        val cont: Continuation<E>?,
        val next: AtomicReference<Receiver<E>> = AtomicReference<Receiver<E>>(null)
    )

    private suspend fun suspend(): E {
        return suspendCoroutine { cont ->
            val node = Receiver(cont)
            while (true) {
                val curTail = deqIdx.get()
                if (curTail.next.compareAndSet(null, node)) {
                    if (deqIdx.compareAndSet(curTail, node)) {
                        break
                    }
                }
            }
        }
    }

    private fun resume(element: E) {
        while (true) {
            val curHead = enqIdx.get()
            if (curHead == deqIdx.get()) {
                continue
            }
            val node = curHead.next.get()
            if (enqIdx.compareAndSet(curHead, node)) {
                node.cont!!.resume(element)
                return
            }
        }
    }

    // ==============
    // Blocking Stack
    // ==============


    private val head = AtomicReference<Node<E>?>()
    private val elements = AtomicInteger()

    override fun push(element: E) {
        val elements = this.elements.getAndIncrement()
        if (elements >= 0) {
            // push the element to the top of the stack
            while (true) {
                val curHead = head.get()
                if (curHead?.element == SUSPENDED) {
                    val node = curHead.next
                    return if (head.compareAndSet(curHead, node)) {
                        resume(element)
                    } else {
                        continue
                    }
                } else if (head.compareAndSet(curHead, Node(element, curHead))) {
                    break
                }
            }
        } else {
            // resume the next waiting receiver
            resume(element)
        }
    }

    override suspend fun pop(): E {
        val elements = this.elements.getAndDecrement()
        if (elements > 0) {
            while (true) {
                // remove the top element from the stack
                val curHead = head.get()
                if (curHead == null) {
                    val node = Node<E>(SUSPENDED, null)
                    return if (head.compareAndSet(curHead, node)) suspend() else continue
                }
                if (head.compareAndSet(curHead, curHead.next)) {
                    return curHead.element as E
                }
            }
        } else {
            return suspend()
        }
    }
}

private class Node<E>(val element: Any?, val next: Node<E>?)

private val SUSPENDED = Any()