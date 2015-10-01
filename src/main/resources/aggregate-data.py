#!/usr/bin/python3

import os, sys, re
import scipy.stats
import numpy

data_path = "D:/dev/code/myo-streaming/src/main/resources/raw-myo-data/";
paths = [os.path.join(data_path,fn) for fn in next(os.walk(data_path))[2]]

header = "label" + ";" + ";".join("mean_"+str(i)+";std_"+str(i)+";min_"+str(i)+";max_"+str(i)+";med_"+str(i)+";kur_"+str(i)+";ske_"+str(i)+";perc25_"+str(i)+";perc75_"+str(i) for i in range(1,14,1))
print(header)
for path in paths:

	try:
		data_org = numpy.genfromtxt(path, delimiter=';')
		data = data_org[:,1:14]

		mean = numpy.mean(data, axis=0)
		std = numpy.std(data, axis=0)
		min = numpy.amin(data, axis=0)
		max = numpy.amax(data, axis=0)
		med = numpy.median(data, axis=0)
		kur = scipy.stats.kurtosis(data, axis=0)
		ske = scipy.stats.skew(data, axis=0)
		perc25 = numpy.percentile(data, 25, axis=0)
		perc75 = numpy.percentile(data, 75, axis=0)

		summary = numpy.array([numpy.concatenate([mean, std, min, max, med, kur, ske, perc25, perc75])])[0]
		isOk = 1.0 if("OK" in os.path.basename(path)) else 0.0
		csv = str(isOk) + ';' + ';'.join(['%.5f' % num for num in summary])
		print(csv)

	except ValueError:
		print('Error while processing file: ' + path, file=sys.stderr)

