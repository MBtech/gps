import os.path
import sys

baseOutputDir = "/home/semih/projects/gps/stats/"
logsDir = "/home/semih/var/logs/"
firstColumnPrefix = "-first-column.txt"
secondColumnPrefix = "-second-column.txt"
statsToTakeMaximumOfWhenMerging = set(["SUPERSTEP_START_TIME", "TOTAL_COMPUTATION_TIME",
                                       "TOTAL_TIME", "DATA_PARSER_FIRST_MESSAGE_PARSING_TIME",
                                       "DATA_PARSER_TIME_BETWEEN_FIRST_AND_LAST_MESSAGES",
                                       "START_TIME", "END_TIME_BEFORE_WRITING_OUTPUT",
                                       "END_TIME_AFTER_WRITING_OUTPUT",
                                       "WAIT_TIME_FOR_TERMINATION_CONTROL_MESSAGES_TO_BE_SENT",
                                       "WAIT_TIME_FOR_TERMINATION_CONTROL_MESSAGES_TO_BE_RECEIVED",
                                       "TOTAL_TIME_BEFORE_WRITING_OUTPUT",
                                       "TOTAL_TIME_AFTER_WRITING_OUTPUT", "EDGE_DENSITY"])
statsToBeFormattedAsFloat  = set(["EDGE_DENSITY", "TOTAL_GC_TIME"])
twentySpaces = "                    "

def main(argv):
    # the parsed stats will go into outputDir/graphName/
    graphName = argv[0]
    filePrefix = argv[1]
    numMachines = argv[2]
    outputDir = baseOutputDir + graphName + "/" + numMachines + "-machines/" + filePrefix + "/"
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # how many nodes that are being shuffled have how many edges (grouped by 20)
    globalMachineStats = GlobalMachineStats()
    for i in range(0, int(numMachines)):
        print 'starting parsing file %d\n' % i
        machineStats = parseLogFile(logsDir + filePrefix + "-machine" + str(i) + "-output.txt", i)
        writeMachineStatsToFile(machineStats, outputDir, filePrefix + "-machine" + str(i), False, int(numMachines))
        mergeMachineStats(machineStats, globalMachineStats)

    writeMachineStatsToFile(globalMachineStats, outputDir, filePrefix + "-global", True, int(numMachines))

def mergeMachineStats(machineStatsSrc, globalMachineStats):
    mergeIntMapOfMaps(machineStatsSrc.machineId, machineStatsSrc.perSuperstepStats, globalMachineStats.perSuperstepStats)
    mergeIntMaps(machineStatsSrc.machineId, machineStatsSrc.globalStats, globalMachineStats.globalStats)
#     mergeArrays(machineStatsSrc.totalGCTime, machineStatsDest.totalGCTime, True)
#     mergeArrays(machineStatsSrc.totalNodeShufflingTime, machineStatsDest.totalNodeShufflingTime, True)
#     mergeArrays(machineStatsSrc.totalNodeShufflesBarrierTime, machineStatsDest.totalNodeShufflesBarrierTime, False)
#     mergeArrays(machineStatsSrc.numNodesToShuffle, machineStatsDest.numNodesToShuffle, False)
#     mergeIntMapOfMaps(machineStatsSrc.numEdgesBracketSizes, machineStatsDest.numEdgesBracketSizes)
#     mergeIntMapOfMaps(machineStatsSrc.numEdgesBracketTotalBenefitPlot, machineStatsDest.numEdgesBracketTotalBenefitPlot)
#     mergeIntMapOfMaps(machineStatsSrc.benefitBracketSizes, machineStatsDest.benefitBracketSizes)
#     mergeIntMapOfMaps(machineStatsSrc.benefitBracketTotalBenefitPlot, machineStatsDest.benefitBracketTotalBenefitPlot)
#     mergeIntMaps(machineStatsSrc.nodesToShuffle, machineStatsDest.nodesToShuffle)
#     mergeIntMapOfMaps(machineStatsSrc.nodesToShufflePerSuperstep, machineStatsDest.nodesToShufflePerSuperstep)

# def convertIntMapToMapOfBracketSizes(map):
#     bracketMap = {}
#     for key in sorted(map):
#         bracket = map[key]
#         mergeKeyValueIntoMap(bracketMap, bracket, 1)
    
#     return bracketMap

# def mergeArrays(arraySrc, arrayDest, takeMax):
#     if len(arraySrc) != len(arrayDest):
#         print'array lengths are not equal to each other: srcLen: %d\tdestLen: %d' % (len(arraySrc),
#                                                                                       len(arrayDest))
#         print 'assuming that the destination array is empty'
#         for i in range(len(arraySrc)):
#             arrayDest.append(arraySrc[i])
#     else:
#         for i in range(len(arraySrc)):
#             if takeMax:
#                 if arraySrc[i] > arrayDest[i]:
#                     arrayDest[i] = arraySrc[i]
#             else:
#                 arrayDest[i] += arraySrc[i]

def mergeIntMapOfMaps(srcMachineId, mapOfMapSrc, mapOfMapDest):
    for superstepNo in sorted(mapOfMapSrc):
        if not mapOfMapDest.has_key(superstepNo):
            mapOfMapDest[superstepNo] = {}

        mergeIntMaps(srcMachineId, mapOfMapSrc[superstepNo], mapOfMapDest[superstepNo])

def mergeIntMaps(srcMachineId, mapSrc, mapDest):
        for key in sorted(mapSrc):
            takeMax = False
            if key in statsToTakeMaximumOfWhenMerging:
                takeMax = True
            mergeKeyValueIntoMap(srcMachineId, mapDest, key, mapSrc[key], takeMax)

def parseLogFile(fileName, machineId):
    numEdgesBracketLength = 20
    benefitBracketLength = 5

    f =  open(fileName)
    machineStats = MachineStats(machineId)
    superstepNo = 0
    finishedSuperstepNo = 0
    lines = f.readlines()
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if line.find("starting superstep") > -1:
            line = line.replace(':', ' ')
            words = line.split()
            superstepNo = int(words[len(words) - 5])
#             machineStats.numNodesToShuffle.append(0)
#             machineStats.totalGCTime.append(0.0);
#             machineStats.totalNodeShufflingTime.append(0)
#             machineStats.totalNodeShufflesBarrierTime.append(0)
#             machineStats.numEdgesBracketSizes[superstepNo] = {}
#             machineStats.numEdgesBracketTotalBenefitPlot[superstepNo] = {}
#             # how many nodes give how much benefit
#             machineStats.benefitBracketSizes[superstepNo] = {}
#             machineStats.benefitBracketTotalBenefitPlot[superstepNo] = {}
#             machineStats.numEdgesBracketTotalBenefitPlot[superstepNo] = {}
#             machineStats.nodesToShufflePerSuperstep[superstepNo] = {}
            print 'new superstep %d\n' % superstepNo 
        elif (line.find("Machine stats for superstepNo") > -1 or
              line.find("Global machine stats") > -1):
            superstepMachineStats = line.find("Machine stats for superstepNo") > -1
            superstepNoForMachineStats = -1
            if superstepMachineStats:
                words = line.replace(':', ' ').split()
                superstepNoForMachineStats = int(words[len(words) - 2])
                if not machineStats.perSuperstepStats.has_key(superstepNoForMachineStats):
                    print 'machineStats.perSuperstepStats did not have the key for superstepNo: %d ' % superstepNoForMachineStats
                    print 'There must have been no garbage collection since last superstep'
                    machineStats.perSuperstepStats[superstepNoForMachineStats] = {}
                    # We add this separately here because when writing stats to file
                    # we assume that for each stat there is an entry for each superstep.
                    # If we don't add this here, we would run into a KeyError when we're writing
                    # stats to file.
                    machineStats.perSuperstepStats[superstepNoForMachineStats]['TOTAL_GC_TIME'] = 0.0
                print 'found machine stats for superstepNo: %d\n' % superstepNoForMachineStats
            i += 1
            line = lines[i]
            while (line.find("End of machine stats for superstepNo: %d" % superstepNoForMachineStats) <= -1 and
                   line.find("End of global machine stats") <= -1):
                if line.find("StatName") > -1:
                    words = line.split()
                    floatValue = -1.0
                    statName = words[len(words) - 3]
                    if words[len(words) - 1] != "null":
                        floatValue = float(words[len(words) - 1])
                        if superstepMachineStats:
                            machineStats.perSuperstepStats[superstepNoForMachineStats][statName] = floatValue
                        else:
                            machineStats.globalStats[statName] = floatValue
                i += 1
                line = lines[i]
            if superstepMachineStats:
                finishedSuperstepNo = superstepNoForMachineStats
        elif line.find("GC") > -1 and line.find("->") > -1 and superstepNo > 0:
            words = line.split()
            if superstepNo == finishedSuperstepNo or superstepNo == (finishedSuperstepNo + 1):
                gcSuperstepNo = superstepNo
                if superstepNo == finishedSuperstepNo:
                    gcSuperstepNo = superstepNo + 1
                    print 'GC in between two supersteps, adding this GC for next superstepNo'          
                if not machineStats.perSuperstepStats.has_key(gcSuperstepNo):
                    machineStats.perSuperstepStats[gcSuperstepNo] = {}
                    machineStats.perSuperstepStats[gcSuperstepNo]['TOTAL_GC_TIME'] = 0.0
                #print 'Adding GC Time for superstepNo: %d gcTime: %f\n' % (gcSuperstepNo, float(words[len(words) -2]))
                machineStats.perSuperstepStats[gcSuperstepNo]['TOTAL_GC_TIME'] += float(words[len(words) -2])
            else:
                print 'Error in parsing GC data: superstepNo is not equal to finishedSuperstepNo or finishedSuperstepNo + 1!\n'
                print 'superstepNo %d\tfinishedSuperstepNo: %d\n' % (superstepNo, finishedSuperstepNo)
#         elif line.find("numNodesToShuffle") > -1:
#             machineStats.numNodesToShuffle[superstepNo - 1] = getLastIntOfLine(line)
#             print 'numNodes per superstepNo:%d\t%d' % (superstepNo, machineStats.numNodesToShuffle[superstepNo - 1])
#         elif line.find("benefitFromNodesToShuffle") > -1:
#             words = line.split(' ')
#             nodeId = int(words[len(words) - 5])
#             numEdges = int(words[len(words) - 3])
#             benefit = int(words[len(words) -1])
#             numEdgesBracket = (numEdges / numEdgesBracketLength) + 1
#             benefitBracket = (benefit / benefitBracketLength) + 1
#             mergeKeyValueIntoMap(machineStats.nodesToShuffle, nodeId, 1)
#             mergeKeyValueIntoMapOfMap(superstepNo, nodeId, 1, machineStats.nodesToShufflePerSuperstep)
#             mergeKeyValueIntoMapOfMap(superstepNo, numEdgesBracket, 1, machineStats.numEdgesBracketSizes)
#             mergeKeyValueIntoMapOfMap(superstepNo, benefitBracket, 1, machineStats.benefitBracketSizes)
#             mergeKeyValueIntoMapOfMap(superstepNo, numEdgesBracket, benefit, machineStats.numEdgesBracketTotalBenefitPlot)
#             mergeKeyValueIntoMapOfMap(superstepNo, benefitBracket, benefit, machineStats.benefitBracketTotalBenefitPlot)
#         elif line.find("Total Node Shuffling Time") > -1:
#             line = line.replace(':', ' ')
#             words = line.split()
#             machineStats.totalNodeShufflingTime[superstepNo -1] = int(words[len(words) -1])
#         elif line.find("Total NodeShuffles Barrier Time") > -1:
#             line = line.replace(':', ' ')
#             words = line.split()
#             machineStats.totalNodeShufflesBarrierTime[superstepNo -1] = int(words[len(words) -1])
#         elif line.find("TotalTimeBeforeWritingResults") > -1:
#             line = line.replace(':', ' ')
#             words = line.split()
#             machineStats.totalFinalTimeBeforeWritingResults = int(words[len(words) -1])
#         elif line.find("TotalTimeAfterWritingResults") > -1:
#             line = line.replace(':', ' ')
#             words = line.split()
#             machineStats.totalFinalTimeAfterWritingResults = int(words[len(words) -1])
        i += 1
    f.close()

    return machineStats

def writeMachineStatsToFile(machineStats, outputDir, filePrefix, isGlobalMachineStats, numMachines):
#     writeArrayToFile(machineStats.totalGCTime,
#                      outputDir +  filePrefix + "-totalGCTime")
#     writeIntMapOfMapToFile(machineStats.numEdgesBracketSizes,
#          outputDir +  filePrefix + "-numEdgesBracketSizes")
#     writeIntMapOfMapToFile(machineStats.benefitBracketSizes,
#          outputDir + filePrefix + "-benefitBracketSizes")
#     writeIntMapOfMapToFile(machineStats.numEdgesBracketTotalBenefitPlot,
#          outputDir + filePrefix + "-numEdgesBracketTotalBenefitPlot")
#     writeIntMapOfMapToFile(machineStats.benefitBracketTotalBenefitPlot,
#           outputDir + filePrefix + "-benefitBracketTotalBenefitPlot")
#     writeIntMapToFile(convertIntMapToMapOfBracketSizes(machineStats.nodesToShuffle),
#           outputDir + filePrefix + "-numTotalShufflesNumNodesShuffled2")
#     writeIntMapOfMapToFile(constructPerSuperstepNumTotalShufflesNumNodesShuffledMap(machineStats.nodesToShufflePerSuperstep,
#           machineStats.nodesToShuffle), outputDir + filePrefix + "-perSuperstepNumTotalShufflesNumNodesShuffled")
#     writeIntMapOfMapToFile(constructPerNumTotalShufflesSuperstepNumNodesShuffledMap(machineStats.nodesToShufflePerSuperstep,
#           machineStats.nodesToShuffle), outputDir + filePrefix + "-perNumTotalShufflesSuperstepNumNodesShuffled")
    for statName in sorted(machineStats.perSuperstepStats[1]):
        file = open(outputDir + filePrefix + "-" + statName + "-per-superstep", 'w')
        if isGlobalMachineStats:
            writeFirstLineOfAllMachinesSummaryFile(file, statName, numMachines)
        for superstepNo in sorted(machineStats.perSuperstepStats):
            if not machineStats.perSuperstepStats[superstepNo].has_key(statName):
                continue
            if not isGlobalMachineStats:
                formatString = "%d\n"
                if statName in statsToBeFormattedAsFloat:
                    formatString = "%f\n"
                file.write(formatString % machineStats.perSuperstepStats[superstepNo][statName])
            else:
                formatString = "%d\t"
                if statName in statsToBeFormattedAsFloat:
                    formatString = "%f\t"
                for key in sorted(machineStats.perSuperstepStats[superstepNo][statName]):
                    file.write(formatString % machineStats.perSuperstepStats[superstepNo][statName][key])
                file.write('\n')
        file.close()
    globalStatsFile = open(outputDir + filePrefix + "-globalstats", 'w')
    for statName in sorted(machineStats.globalStats):
        if isGlobalMachineStats:
            globalStatsFile.write("%s\n" % statName)
            writeFirstLineOfAllMachinesSummaryFile(globalStatsFile, statName, numMachines)
        if not isGlobalMachineStats:
            formatString = "%d\n"
            if statName in statsToBeFormattedAsFloat:
                formatString = "%f\n"
            # This formatting to align the values on a single column. We're building a long string
            # with the statName followed by a lot of white space and then trimming to exactly 60
            # characters and then adding the statValue. This way, the statValues all start from 61st
            # character.
            globalStatsFile.write(("%s                                                          " % statName)[0:60])
            globalStatsFile.write(formatString % machineStats.globalStats[statName])
        else:
            formatString = "%d%s"
            if statName in statsToBeFormattedAsFloat:
                formatString = "%f%s"
            for key in sorted(machineStats.globalStats[statName]):
                globalStatsFile.write((formatString % (machineStats.globalStats[statName][key], twentySpaces))[0:20])
            globalStatsFile.write('\n\n')
    globalStatsFile.close()

# Note: This is a very confusing function. It does the following: nodesToShufflePerSuperstep
# is a map-of-map from superstep->nodeId->1 (this 1 is just an indication that this nodeId
# was shuffled once in this superstep (which by definition is 1 but this data is kept in this
# form just because I was lazy to implement it correctly)
# nodesToShuffle is a map from nodeId->numTimesShuffled.
# What we want to compute is the following:
# per superstep, how many of the nodes that got shuffled x times got shuffled in this superstep
# so something of the form:
# 1 -> 1 -> 1000 (out of all the nodes that got shuffled once, 1000 of them got shuffled in superstep 1)
# 1 -> 2 -> 890 (out of all the nodes that got shuffled twice, 890 of them got shuffled in superstep 2)
# 1 -> 3 -> 200
# 2 -> 1 -> 300 (out of all the nodes that got shuffled once, 300 of them got shuffled in superstep 2), etc.
# def constructPerSuperstepNumTotalShufflesNumNodesShuffledMap(nodesToShufflePerSuperstep,
#                                                              nodesToShuffle):
#     superstepNumTotalShufflesNumNodesShuffledMap = {}
#     for superstepNo in sorted(nodesToShufflePerSuperstep):
#         superstepNumTotalShufflesNumNodesShuffledMap[superstepNo] = {}
#         nodesToShuffleMap = nodesToShufflePerSuperstep[superstepNo]
#         for nodeToShuffle in nodesToShuffleMap:
#             numTimesShuffled = nodesToShuffle[nodeToShuffle]
#             mergeKeyValueIntoMapOfMap(superstepNo, numTimesShuffled, 1, superstepNumTotalShufflesNumNodesShuffledMap)

#     return superstepNumTotalShufflesNumNodesShuffledMap

# def constructPerNumTotalShufflesSuperstepNumNodesShuffledMap(nodesToShufflePerSuperstep,
#                                                              nodesToShuffle):
#     numTotalShufflesSuperstepNumNodesShuffledMap = {}
#     for superstepNo in sorted(nodesToShufflePerSuperstep):
#         for nodeToShuffle in nodesToShufflePerSuperstep[superstepNo]:
#             numTimesShuffled = nodesToShuffle[nodeToShuffle]
#             if numTimesShuffled not in numTotalShufflesSuperstepNumNodesShuffledMap:
#                 numTotalShufflesSuperstepNumNodesShuffledMap[numTimesShuffled] = {}
#             mergeKeyValueIntoMapOfMap(numTimesShuffled, superstepNo, 1, numTotalShufflesSuperstepNumNodesShuffledMap)

#     return numTotalShufflesSuperstepNumNodesShuffledMap

# def writeIntToFile(intValue, fileNamePrefix):
#     file = open(fileNamePrefix, 'w')
#     file.write("%s\n" % intValue)
#     file.close()

# def writeArrayToFile(array, fileNamePrefix):
#     firstColumnFile = open(fileNamePrefix + firstColumnPrefix, 'w')
#     secondColumnFile = open(fileNamePrefix + secondColumnPrefix, 'w')
#     for i in range(len(array)):
#         firstColumnFile.write("%s\n" % str(i))
#         secondColumnFile.write("%s\n" % str(array[i]))

#     firstColumnFile.close()
#     secondColumnFile.close()
# OA
# def writeIntMapOfMapToFile(mapOfMap, fileNamePrefix):
#     firstColumnFile = open(fileNamePrefix + firstColumnPrefix, 'w')
#     secondColumnFile = open(fileNamePrefix + secondColumnPrefix, 'w')
#     for superstepNo in sorted(mapOfMap):
#         firstColumnFile.write("MapKey: %d\n" % superstepNo)
#         secondColumnFile.write("MapKey: %d\n" % superstepNo)
#         writeIntMapToFiles(mapOfMap[superstepNo], firstColumnFile, secondColumnFile)

#     firstColumnFile.close()
#     secondColumnFile.close()

# def writeIntMapToFile(map, fileNamePrefix):
#     writeIntMapToFiles(map, open(fileNamePrefix + firstColumnPrefix, 'w'),
#                            open(fileNamePrefix + secondColumnPrefix, 'w'))

# def writeIntMapToFiles(map, firstColumnFile, secondColumnFile):
#     for key in sorted(map):
#         firstColumnFile.write("%d\n" % key)
#         secondColumnFile.write("%d\n" % map[key])

def writeFirstLineOfAllMachinesSummaryFile(file, statName, numMachines):
    formatString = "%s%s"
    for i in range(numMachines):
        file.write((formatString % (('Machine#%d' % i), twentySpaces))[0:20])
    if statName in statsToTakeMaximumOfWhenMerging:
        file.write('Max\n')
    else:
        file.write('Total\n')

def mergeKeyValueIntoMapOfMap(srcMachineId, firstKey, key, value, mapOfMapOfMap, takeMax):
    mapOfMap = mapOfMapOfMap[firstKey]
    mergeKeyValueIntoMap(srcMachineId, mapOfMap, key, value, takeMax)

def mergeKeyValueIntoMap(srcMachineId, mapOfMap, key, value, takeMax):
    if not mapOfMap.has_key(key):
        mapOfMap[key] = {}

    mapOfMap[key][srcMachineId] = value
    if takeMax:
        if not mapOfMap[key].has_key('max'):
            mapOfMap[key]['max'] = value
        elif value > mapOfMap[key]['max']:
            mapOfMap[key]['max'] = value
    else:
        if not mapOfMap[key].has_key('total'):
            mapOfMap[key]['total'] = value
        else:
            mapOfMap[key]['total'] = mapOfMap[key]['total'] + value

# def getLastIntOfLine(line):
#     words = line.split()
#     return int(words[len(words) - 1])

class GlobalMachineStats:
    def __init__(self):
        self.perSuperstepStats = {}
        self.globalStats = {}

class MachineStats:
    def __init__(self, machineId):
        self.machineId = machineId
        self.perSuperstepStats = {}
        self.globalStats = {}
#         self.totalGCTime = []
#         self.totalNodeShufflingTime = []
#         self.totalNodeShufflesBarrierTime = []
#         self.numEdgesBracketSizes = {}
#         self.numEdgesBracketTotalBenefitPlot = {}
#         # how many nodes give how much benefit
#         self.benefitBracketSizes = {}
#         self.benefitBracketTotalBenefitPlot = {}
#         self.numNodesToShuffle = []
#         self.nodesToShuffle = {}
#         self.nodesToShufflePerSuperstep = {}

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


