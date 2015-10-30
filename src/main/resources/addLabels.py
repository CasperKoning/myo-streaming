#!/usr/bin/python3

import os, fnmatch

inputDirectory = "D:\\dev\\code\\myo-streaming\\src\\main\\resources\\raw-myo-data";
outputDirectory = "D:\\dev\\code\\myo-streaming\\src\\main\\resources\\myo-data-with-label";

for path, dirs, files in os.walk(os.path.abspath(inputDirectory)):
    for filename in fnmatch.filter(files, "*.json"):
        filepath = os.path.join(path, filename)
        label = filename.replace(".json","")
        with open(filepath) as f:
            s = f.read();
        s = s.replace("{","{\"label\":"+label+".0,")
        outputFile = outputDirectory +"\\" + filename;
        with open(outputFile, "w+") as f:
            f.write(s);