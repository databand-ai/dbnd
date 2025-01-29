/*
 * © Copyright Databand.ai, an IBM Company 2025
 */

package ai.databand.examples

import ai.databand.annotations.Task

import scala.util.Random

/**
 * Very simple pipeline used to debug instrumentation and performing sanity checks without Spark-related heavy-lifting.
 */
object SimpleScalaPipeline {

    @Task("simple_scala_pipeline")
    def doStuff(firstValue: Int, secondValue: String): Unit = {
        val firstResult = stepOne(firstValue, secondValue)
        val secondResult = stepTwo(firstResult)
        stepTwo(secondResult.toString)
        processData(secondResult)
        nullTask(null)
        try {
            badTask(secondResult)
        } catch {
            case e: Exception => // do nothing
        }
    }

    @Task
    def processData(data: Int): Unit = {
        // do nothing
    }

    @Task
    def nullTask(value: Any): Any = {
        null
    }

    @Task
    def stepOne(first: Int, second: String): String = {
        first + second
    }

    @Task
    def stepTwo(third: String): Int = {
        val value = firstNestedTask(third.hashCode)
        val value2 = secondNestedTask(third.hashCode + Random.nextInt(10000))
        third.hashCode + value + value2
    }

    @Task
    def badTask(input: Double): Unit = {
        if (input > 1) {
            throw new RuntimeException("Error!")
        }
    }

    @Task
    def firstNestedTask(input: Int): Int = {
        input * 2
    }

    @Task
    def secondNestedTask(input: Int): Int = {
        thirdNestedTask((input + 1000).toString())
        input * 2
    }

    @Task
    def thirdNestedTask(input: String): String = {
        input + input.hashCode
    }
}
