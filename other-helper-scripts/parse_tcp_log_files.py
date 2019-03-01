import os.path
import sys

baseOutputDir = "/home/semih/projects/pregel/stats/"
logsDir = "/home/semih/var/logs/"
firstColumnPrefix = "-first-column.txt"
secondColumnPrefix = "-second-column.txt"

def main(argv):
    # the parsed stats will go into outputDir/graphName/
    numMachines = argv[0]
    tryNo = argv[1]

    outputDir = baseOutputDir + "tcp_sender/" + numMachines + "-machines/" + tryNo + "/"
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    globalMachineStats = MachineStats(-1)
    for i in range(0, int(numMachines)):
        machineStats = parseLogFile(logsDir + "tcp_sending_receiving_tester_mina_64MB_" + numMachines +
                                    "machines_machine"+ str(i) + "_run_" + tryNo + ".txt", i)
        writeMachineStatsToFile(machineStats, outputDir + "machine" + str(i))
        mergeMachineStats(machineStats, globalMachineStats)

    writeMachineStatsToFile(globalMachineStats, outputDir + "global")

def mergeMachineStats(machineStatsSrc, machineStatsDest):
    if machineStatsSrc.sendingTime > machineStatsDest.sendingTime:
        machineStatsDest.sendingTime = machineStatsSrc.sendingTime
        machineStatsDest.idOfSlowestSendingTimeMachine = machineStatsSrc.machineId

    if machineStatsSrc.totalTime > machineStatsDest.totalTime:
        machineStatsDest.totalTime = machineStatsSrc.totalTime
        machineStatsDest.idOfSlowestTotalTimeMachine = machineStatsSrc.machineId

def parseLogFile(fileName, machineId):
    f =  open(fileName)
    machineStats = MachineStats(machineId)
    for linewithnewline in f.readlines():
        line = linewithnewline.rstrip()
        if line.find("time taken to send messages") > -1:
            line = line.replace(':', ' ')
            words = line.split()
            machineStats.sendingTime = getLastIntOfLine(line)
            print 'found time taken to send messages %d\n' % machineStats.sendingTime 
        elif line.find("Total Time") > -1:
            machineStats.totalTime = getLastIntOfLine(line)
            print 'found total time %d\n' % machineStats.totalTime
    f.close()

    return machineStats

def writeMachineStatsToFile(machineStats, outputFileName):
    file = open(outputFileName, 'w')
    file.write("sending time:\t%d" % machineStats.sendingTime)
    if machineStats.idOfSlowestSendingTimeMachine > -1:
        file.write("\tslowest machine: %d\n" % machineStats.idOfSlowestSendingTimeMachine)
    else:
        file.write("\n")
    file.write("total time:\t%d" % machineStats.totalTime)
    if machineStats.idOfSlowestTotalTimeMachine > -1:
        file.write("\tslowest machine: %d\n" % machineStats.idOfSlowestTotalTimeMachine)
    else:
        file.write("\n")


def getLastIntOfLine(line):
    words = line.split()
    return int(words[len(words) - 1])

class MachineStats:
    def __init__(self, machineId):
        self.machineId = machineId
        self.sendingTime = -1
        self.totalTime = -1
        self.idOfSlowestTotalTimeMachine = -1
        self.idOfSlowestSendingTimeMachine = -1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
