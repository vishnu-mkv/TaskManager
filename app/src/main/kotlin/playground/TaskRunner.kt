package playground

import kotlin.io.println
import kotlin.comparisons.compareBy
import kotlin.comparisons.compareByDescending
import kotlinx.coroutines.*

enum class Priority(val value: Int) {
    LOW(1),
    MEDIUM(2),
    HIGH(3)
}

enum class Status {
    NOT_STARTED,
    WAITING_FOR_RETRY,
    RUNNING,
    SUCCESS,
    FAILURE
}

data class Task(val name: String, 
                val priority: Priority, 
                val Fn: suspend () -> Unit, 
                val retry: Int = 2) {

    private val _dependants: MutableList<Task> = mutableListOf()
    var status = Status.NOT_STARTED
        private set  
    private val started: Boolean
        get() = status != Status.NOT_STARTED && status != Status.WAITING_FOR_RETRY
    var retryRemaining = retry
        private set

    fun next(vararg tasks: Task) {
        if(started) throw Exception("Task already started")
        _dependants.addAll(tasks)
    }

    fun run(onResult: (Status) -> Unit = {}) {
        if(started) throw Exception("Task already started")
        status = Status.RUNNING
        try {
            runBlocking {
                // wait for task to complete
                Fn.invoke()
            }
            status = Status.SUCCESS
        } catch (e: Exception) {
            if(retryRemaining > 0) {
                status = Status.WAITING_FOR_RETRY
                retryRemaining--
            } else {
                status = Status.FAILURE
            }
        } finally {
            onResult(status)
        }
    }

    val hasDependants: Boolean
        get() = _dependants.isNotEmpty()

    val dependants: List<Task>
        get() = _dependants.toList()
}

class TaskRunner(val tasks :List<Task> = emptyList()) {

    companion object TaskManager {
        private val waitingQueue = mutableListOf<Task>()
        private val dlq = mutableListOf<Task>()
        private var isRunning = false

        val deadLetterQueue: List<Task>
            get() = dlq.toList()

        fun addTask(vararg tasks: Task) {
            if(isRunning) throw Exception("TaskRunner already running")
            _addTask(*tasks)
        }

        private fun _addTask(vararg tasks: Task) {
            waitingQueue.addAll(tasks)
        }

        fun start(maxRetries: Int = Float.POSITIVE_INFINITY.toInt()) {
            if(isRunning) throw Exception("TaskRunner already running")

            var retryCount = 0
            isRunning = true
            while(waitingQueue.isNotEmpty() && retryCount <= maxRetries) {
                val runner = TaskRunner(waitingQueue.toList())
                waitingQueue.clear()
                runner.run()
                retryCount++
            } 
            isRunning = false  
        }
    }

    private val _tasks = tasks.toMutableList()

    fun run() {

        // order tasks by priority in descending order
        _tasks.sortWith(compareByDescending({ it.priority.value }))

        // remove task from list and run it
        // if task fails, add it to the dead letter queue
        // if task succeeds, create a new TaskRunner and run it

        while (_tasks.isNotEmpty()) {
            val task = _tasks.removeAt(0)
            println("Running task ${task.name} (priority: ${task.priority}, retryRemaining: ${task.retryRemaining})")
            task.run { status ->
                when(status) {
                    Status.SUCCESS -> {
                        println("Task ${task.name} completed successfully")
                        if(task.hasDependants) {
                            println("Task ${task.name} has dependants, running them now")
                            val runner = TaskRunner(task.dependants)
                            runner.run()
                        }
                    }
                    Status.FAILURE -> {
                        println("Task ${task.name} failed permanently")
                        dlq.add(task)
                    }
                    Status.WAITING_FOR_RETRY -> {
                        println("Task ${task.name} failed, will retry")
                        TaskManager._addTask(task)
                    }
                    else -> throw Exception("Invalid status")
                }
            }
        }
    }
}

fun createFn(name: String) : suspend () -> Unit {
    return {
        // delay for random time  1-3 seconds
        val delayTime = (1..3).random() * 1000L
        // throw exception 1/3 of the time
        if ((1..2).random() == 1) throw Exception("Random exception")
        delay(delayTime)
        println("This is from task $name")
    }
}

fun start() {

    // create tasks
    val T1 = Task("Task1", Priority.LOW, createFn("Task1"))
    val T2 = Task("Task2", Priority.MEDIUM, createFn("Task2"))
    val T3 = Task("Task3", Priority.HIGH, createFn("Task3"))
    val T4 = Task("Task4", Priority.LOW, createFn("Task4"))
    val T5 = Task("Task5", Priority.MEDIUM, createFn("Task5"))
    val T6 = Task("Task6", Priority.HIGH, createFn("Task6"))
    val T7 = Task("Task7", Priority.LOW, createFn("Task7"))
    val T8 = Task("Task8", Priority.MEDIUM, createFn("Task8"))
    val T9 = Task("Task9", Priority.HIGH, createFn("Task9"))
    val T10 = Task("Task10", Priority.LOW, createFn("Task10"))

    // create dependancies
    T1.next(T2, T3)
    T3.next(T5, T6)
    T4.next(T7)
    T5.next(T8)
    T9.next(T10)


    TaskRunner.addTask(T1, T4, T9)
    TaskRunner.start()
}



