package org.apache.openwhisk.core.containerpool

import java.util.concurrent.ConcurrentHashMap


// ActionStats data structure
// IAT and CV are provided by controller (measured at request arrival time)
// This is more accurate than measuring at invoker execution completion time
case class ActionStats(
    count: Long, 
    coldCount: Long, 
    avgInit: Double, 
    avgRun: Double, 
    iat: Double,      // Latest IAT from controller
    cv: Double        // Latest CV from controller
) {
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

        // IAT and CV are from controller - use the latest values
        // Controller uses Welford's algorithm for accurate CV calculation
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
                // create new - first invocation
                ActionStats(
                    count = 1, 
                    coldCount = if(newInit > 0) 1 else 0, 
                    avgInit = newInit, 
                    avgRun = newRun, 
                    iat = newIat,  // From controller
                    cv = newCv     // From controller
                )
            } else {
                // update - IAT and CV are from controller (measured at request arrival time)
                currentStats.update(newInit, newRun, newIat, newCv)
            }
        })
    }
}