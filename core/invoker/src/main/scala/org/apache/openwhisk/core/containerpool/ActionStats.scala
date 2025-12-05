package org.apache.openwhisk.core.containerpool

import java.util.concurrent.ConcurrentHashMap


// ActionStats data structure
case class ActionStats(count: Long, coldCount: Long, avgInit: Double, avgRun: Double, iat: Double, cv: Double) {
    // coldstart sensitivity: init time / runtime
    def coldstartSensitivity: Double = if (avgRun > 0) { avgInit / avgRun } else 0.0 

    def update(newInit: Double, newRun: Double, newIat: Double, newCv: Double): ActionStats = {
        val newCount = count + 1
        val isCold = newInit > 0
        val newColdCount = coldCount + (if (isCold) 1 else 0)

        // Only update avgInit if it's a coldstart
        val newAvgInit = if (isCold) {
            avgInit + (newInit - avgInit) / newColdCount
        } else {
            avgInit
        }

        val newAvgRun = avgRun + (newRun - avgRun) / newCount

        ActionStats(newCount, newColdCount, newAvgInit, newAvgRun, newIat, newCv)
    }
}

// Singleton Object
// Stores the global state of all actions
object ActionStatsManager {
    private val stats = new ConcurrentHashMap[String, ActionStats]()

    def get(actionName: String): Option[ActionStats] = {
        Option(stats.get(actionName))
    }

    def update(actionName: String, newInit: Double, newRun: Double, newIat: Double, newCv: Double): ActionStats = {
        stats.compute(actionName, (_, currentStats) => {
            if (currentStats == null) {
                // create new
                ActionStats(1, if(newInit > 0) 1 else 0, newInit, newRun, newIat, newCv)
            } else {
                // update
                currentStats.update(newInit, newRun, newIat, newCv)
            }
        })
    }
}