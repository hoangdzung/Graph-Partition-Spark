import re
import sys 

remoteBytesRead = 0
resultSize = 0
for line in open(sys.argv[1]):
    if '"Event":"SparkListenerStageCompleted"' not in line:
        continue
    m = re.search('(?<=\"internal\.metrics\.shuffle\.read\.remoteBytesRead\"\,\"Value\"\:)[0-9]+', line)
    if m is not None:
        print("remoteBytesRead", int(m.group(0)))
    m = re.search('(?<=\"internal\.metrics\.resultSize\"\,\"Value\"\:)[0-9]+', line)
    if m is not None:
        print("resultSize",int(m.group(0)))