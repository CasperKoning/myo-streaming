#!/usr/bin/python3

import os, fnmatch

from decimal import Decimal

inputDirectory = "D:\\dev\\code\\myo-streaming\\src\\main\\resources\\raw-myo-data";
outputDirectory = "D:\\dev\\code\\myo-streaming\\src\\main\\resources\\myo-data-with-label-classification";

for path, dirs, files in os.walk(os.path.abspath(inputDirectory)):
    for filename in fnmatch.filter(files, "*.json"):
        filepath = os.path.join(path, filename)
        rotations = Decimal(filename.replace(".json",""))
        label = "not moving" if rotations < 2000 else ("slow" if rotations >= 2000 and rotations < 4000 else ("medium" if rotations >= 4000 and rotations < 6000 else ("fast" if rotations >= 6000 and rotations < 8000 else "really fast")))
        with open(filepath) as f:
            s = f.read();
        s = s.replace("{","{\"label\":"+"\""+label+"\""+",")
        outputFile = outputDirectory +"\\" + filename;
        with open(outputFile, "w+") as f:
            f.write(s);