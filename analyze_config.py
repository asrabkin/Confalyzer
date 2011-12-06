# (c) Copyright 2011he Regents of the University of California
# Portions (c) Copyright 2011 Cloudera, Inc.
#  See the file COPYING for license information
#

import os
import glob
import re
import shutil
import subprocess
import time
import hconfParser 
import sys
from optparse import OptionParser

MAPRED_MAINS = {
'TaskTracker': "org.apache.hadoop.mapred.TaskTracker",
'JobTracker': "org.apache.hadoop.mapred.JobTracker",
'TT_Child': "org.apache.hadoop.mapred.Child",
'JobClient': "org.apache.hadoop.mapred.JobClient"
}

HDFS_MAINS = {
'DataNode': "org.apache.hadoop.hdfs.server.datanode.DataNode",
'NameNode': "org.apache.hadoop.hdfs.server.namenode.NameNode",
'FS_Shell': "org.apache.hadoop.fs.FsShell",
'DFSAdmin': "org.apache.hadoop.hdfs.tools.DFSAdmin",
'SecondaryNN': "org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode"
}

HBASE_MAINS = {
"RegionServer":"org.apache.hadoop.hbase.regionserver.HRegion",
"HMaster":"org.apache.hadoop.hbase.master.HMaster"
}


#additional options to pass to chord for a particular daemon, if any.
# Used to add the list of externally-invoked interfaces
HOPTS = { 'DataNode': "", 
"NameNode": "-Dchord.entrypoints=org.apache.hadoop.hdfs.server.namenode.JspHelper"+
",org.apache.hadoop.hdfs.server.namenode.NameNode", 
"TT_Child": "-Dchord.entrypoints=org.apache.hadoop.mapred.JobConfigurable",
"JobTracker":"-Dchord.entrypoints=org.apache.hadoop.mapred.JobTracker"+",org.apache.hadoop.mapred.JobConf" +
",org.apache.hadoop.mapred.JobClient,org.apache.hadoop.mapreduce.JobContext"+",org.apache.hadoop.mapred.JSPUtil"}

#-Dchord.entrypoints="org.apache.hadoop.io.BloomMapFile"
#,org.apache.hadoop.JobConfigurable

#Used to just scan past results instead of re-running the analysis.
#Intended purely for debugging the output formatting.
doAnalysis = True
GREP_APIDOCS = False

USAGE="%prog <hadoop or hbase jar>  [options] [--dynamic=<file>]"
#[onlyhdfs|onlymapred|onlyhbase]
def main():
	global HMAINS,doAnalysis
	
	parser = OptionParser(usage=USAGE)
	parser.add_option("-r", "--rescan", action="store_false", dest="doAnalysis", default=True, \
		help="don't re-run jchord, use cached results")
	parser.add_option("--dynamic", "-d", action="store", type="string", dest="dynfile", \
	  help = "use dynamic-analysis output FILENAME")
	parser.add_option("--onlymapred", action="store_true", dest="onlymapred", \
	  help = "only analyze MR daemons")
	parser.add_option("--onlyhdfs", action="store_true", dest="onlyhdfs", \
	  help = "only analyze HDFS daemons")
	parser.add_option("--onlyhbase", action="store_true", dest="onlyhbase", \
	  help = "only analyze HBase daemons")

	(options, args) = parser.parse_args()
	doAnalysis = options.doAnalysis
	
	if not doAnalysis:
		print "skipping actual analysis, just analyzing past results"

	if len(args) != 1:
		print "python",USAGE,"\n Use --help for more information"
		sys.exit(1)
	jarPath = os.path.abspath(args[0])

	outName = "hadoop-configuration.html"
	if options.onlymapred:
		HMAINS = MAPRED_MAINS
	elif options.onlyhdfs:
		HMAINS = HDFS_MAINS
	elif options.onlyhbase:
		HMAINS = HBASE_MAINS
		outName = "hbase-configuration.html"
	else:
		HMAINS = {}
		HMAINS.update(HDFS_MAINS)
		HMAINS.update(MAPRED_MAINS)
		
	hadoopDir = os.path.dirname(jarPath)
	libJarPath = getLibsFromJarPath(hadoopDir,jarPath)
#	print "lib path is",libJarPath
	jarVersion = getVersionFromJar(libJarPath)
	print "Jar version was",jarVersion
	jarDefaults = getJarOptionDefaults(jarPath)
	
	if options.dynfile:
		dyn_read = read_dyn(options.dynfile)
		print "using dynamic behavior from",options.dynfile,"total of",len(dyn_read),"dynamic options"
	else:
		dyn_read = {}
	
	if len(jarVersion) < 3:
		print "failed to find jar version, aborting."
		sys.exit(1)
		
	

	for libJar in libJarPath.split(':'):
		baseName = os.path.basename(libJar)
		if "hadoop" in baseName:
			print "found supplemental Hadoop jar",baseName
			jarDefaults.update( getJarOptionDefaults(libJar) ) 

	
	print len(jarDefaults),"options had a default value set"
	readPoints = {} #map from opt name to list of places & daemons where read
	writePoints = {} #parallel to read points
	codeDefaults = {}
	for (k,v) in sorted(HMAINS.items()):
		#run the command
		chordOutDir =  k+"_output"
		
		if doAnalysis:
			runChordAnalysis(k,v, chordOutDir, libJarPath)			
			#rename/cleanup
			#generate output
		myOptCount = updateOptList(k, chordOutDir,readPoints, "conf_regex.txt")
		print "Static analysis found",myOptCount,"options potentially read by",k
		myOptCount = updateOptList(k, chordOutDir,writePoints, "conf_writes.txt")
		print "Static analysis found",myOptCount,"options potentially written by",k

		codeDefaults.update(getCodeOptionDefaults(chordOutDir))

	jarAndDocsDefaults = getDocsFromDir(hadoopDir, jarDefaults,readPoints)
	print "total of",len(writePoints),"options written"
	dumpHTML(readPoints, writePoints, jarAndDocsDefaults, jarVersion, codeDefaults, outName,dyn_read)
	dumpNewDictionary(readPoints, dyn_read,jarDefaults)

def getVersionFromJar(jarPath):
	if 'hbase' in jarPath:
		version_info_class =  "org.apache.hadoop.hbase.util.VersionInfo"
	else:
		version_info_class = "org.apache.hadoop.util.VersionInfo"
	
	print "running java -cp",jarPath,version_info_class
	p = subprocess.Popen(["java", "-cp", jarPath, version_info_class], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	(version, err) = p.communicate()
	return version

def getLibsFromJarPath(hadoopDir,jarPath):
	"""Returns a java classpath, including the specified jar and any others in
	 ../lib directories alongside it"""
	libPath = [jarPath]
	libJars = os.path.join(hadoopDir, "lib/*.jar")
	jarList = glob.glob(libJars)
	libPath.extend(jarList)
	ivyJars = os.path.join(hadoopDir,"build/ivy/lib/*adoop*/common/*.jar")
	jarList = glob.glob(ivyJars)
	libPath.extend(jarList)
	
	joinedLibPath = ":".join(libPath)
#	print "lib jar path is",joinedLibPath
	return joinedLibPath


def getDocsFromDir(hadoopDir, jarDefaults, readPoints):
	jarAndCodeDefaults = {}
	jarAndCodeDefaults.update(jarDefaults)
	apiDocPath = os.path.join(hadoopDir, "docs/api")
	docHtml = glob.glob(os.path.join(hadoopDir, "docs/*.html"))
	print "Grepping through docs for options; this may take a while. Started",time.ctime()
	for optName in sorted(readPoints):
		if optName in jarDefaults:
			continue
		if optName.startswith('CONF-'):
			shortOptName = optName[5:]
		else:
			continue
			
		if not re.match('[a-zA-Z]{3}', shortOptName): #ignore names that are all regex
			continue
		
		args = ["grep", "-R", "--max-count=1", shortOptName]
		if os.path.exists(apiDocPath) and GREP_APIDOCS:
			args.extend(apiDocPath)

		args.extend(docHtml)
		p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(text, err) = p.communicate()
		if len(err) > 0:
			print "Grep failed:",err
			sys.exit(0)
			
		if len(text) > 0:
			(file,_,desc)=text.partition(':')
			desc = desc.replace("<", "&lt;")
			desc = desc.replace(">", "&gt;")

			jarAndCodeDefaults[optName] = ("", desc, os.path.relpath(file, hadoopDir))
	print "Done grepping for config options",time.ctime()
	return jarAndCodeDefaults


def getJarOptionDefaults(jarPath):
	"""Returns a map from option name to a (value, description,source xml file) pair."""
	if os.path.exists("tmp_opt"):
		shutil.rmtree("tmp_opt")

	os.mkdir("tmp_opt")
	os.chdir("tmp_opt")
	print "running jar -xf " + jarPath+ " core-default.xml mapred-default.xml hdfs-default.xml hbase-default.xml"
	os.system("jar -xf "+jarPath+ " core-default.xml mapred-default.xml hdfs-default.xml hbase-default.xml")
	results = {}
	for fname in ["core-default.xml","mapred-default.xml", "hdfs-default.xml", "hbase-default.xml"]:
		if os.path.exists(fname):
			print "found defaults file",fname
			(defaultVals, descriptions) = hconfParser.getOptsFromFile(fname)
			for (k,v) in defaultVals.iteritems():
				results["CONF-"+ k] = (v, descriptions[k],fname)
			
	os.chdir("..")
	shutil.rmtree("tmp_opt")
	return results
		#read file from fname

def getCodeOptionDefaults(chordOutDir):
	opts = {}
	defaultsFile = open(chordOutDir + '/default_conf_vals.txt', 'r')
	for line in defaultsFile:
		words = line.split("\t")
		optName = words[0]
		defVal = words[1]
		opts[optName] = defVal
	defaultsFile.close()
	return opts

def read_dyn(dynfile):
	dyn_read = {}
	f = open(dynfile,'r')
	for ln in f:
		(name,list) = ln.split(" ")
		dyn_read["CONF-" + name] = list.strip().split(',')
	f.close()
	return dyn_read


n = 1
def runChordAnalysis(name, mainClass, chordOutDir, jarPath):
	global n
	opts = HOPTS.get(name, "")
	numLine = "(" + str(n) + "/"+ str(len(HMAINS)) + ")"
	n+=1
	print "---\t Analysis of ",name, numLine, "started at",time.ctime()
	if len(opts) > 0:
		cmd = "nice -n 5 java -Dchord.main.class=" + mainClass + " -Dchord.class.path="+jarPath + " " + opts + " -Dchord.props.file=chord.properties -jar chord.jar"
	else:
			cmd = "nice -n 5 java -Dchord.main.class=" + mainClass+ " -Dchord.class.path="+jarPath +" -Dchord.props.file=chord.properties -jar chord.jar"
	print "\texecuting",cmd
	#run the command
	t = time.time()
	rc = os.system(cmd)
	t = time.time() -t
	print "---\t",name,"done, exit status ", rc," Time elapsed:",int(t),"secs."
	if rc != 0:
		print "Something went wrong!  Check the last few entries in chord_output/log.txt to see what."
		exit(rc)
	
	if os.path.exists(chordOutDir):  #remove any results from previous run
		shutil.rmtree(chordOutDir)
	shutil.rmtree("chord_output/bddbddb") #Now remove the analysis intermediates
	shutil.rmtree("chord_output/dlog")
	os.rename('chord_output',chordOutDir) #And save what's left


def updateOptList(daemonName, chordOutDir,readPoints, fName):
	"""Given a regex filename and directory, updates readPoints.
	ReadPoints should be a map from option name to a list of code points.
	Responsible for shortening names of code."""
	regexFile = open(chordOutDir + '/'+ fName, 'r')	
	# could slurp options.dict and join the two
	
	myOptCount = 0
	for line in regexFile:
		words = line.split(" ")
		optName = words[0]
		shortPos = words[3].replace("org.apache.hadoop", "o.a.h")
		methname = words[4].replace("<init>", "&lt;init&gt;")
		optPt = daemonName + " " + shortPos+"." + methname
		if optName in readPoints:
			readPoints[optName].append(optPt)
		else:
			readPoints[optName] = [optPt]
		myOptCount += 1
	regexFile.close()
	return myOptCount

substPat = re.compile("\\$\\{([^\\}\\$\u0020]+)\\}")
def findSubstitutionUses(jarDefaultVals):
	ret = set({})
	for o in jarDefaultVals:
		m = substPat.search(o)
		if m:
			ret.add("CONF-" + m.group(1))
	return ret

def findUnusedOpts(readPoints,jarDefaults):
	"""Returns a pair: unused,falsepos
	unused is a list of options that have defaults but don't seem to exist in the code.
	falsepos is options that were read dynamically but not found by static analysis.
	"""
	regexOptList = filter( lambda x: '*' in x  , readPoints.keys())
	usedBySubstitution = findSubstitutionUses( map( lambda x: x[0], jarDefaults.values()))
	print "findSubstitutionUses had",len(usedBySubstitution),'hits'
	unmatched = []
	for o in jarDefaults:
		if o in readPoints or o in usedBySubstitution: #precise match, move on
			continue
		else:
			matched = False
			for r in regexOptList:
				if re.match(r, o):
					matched = True
					break
			
			if not matched:
				unmatched.append(o)
	return unmatched


def mergeStatAndDynData(staticReadPoints,dyn_read, jarDefaults):
	"""Merges static and dynamic results, comparing both against jar defaults.
	Returns the list of real readpoints,
	plus a list of cases where the static analysis wrongly decided an option did not exist"""
	readPoints = {}
	readPoints.update(staticReadPoints)
	falsePos = []
	usedBySubstitution = findSubstitutionUses( map( lambda x: x[0], jarDefaults.values()))

	regexOptList = filter( lambda x: '*' in x  , readPoints.keys())
	for o in dyn_read:
		if o in readPoints or o in usedBySubstitution: #precise match, move on
			continue
		else:
			matched = False
			for r in regexOptList:
				if re.match(r, o):
					matched = True
					break
			if not matched:
				falsePos.append(o)
				daemonList = dyn_read[o]
				readPoints[o] = map(lambda dname: dname + " dynamic", daemonList)
	
	return readPoints,falsePos

def dumpCSV(readPoints, jarDefaults, jarVersion, codeDefaults):
	optNames = readPoints.keys()
	optNames.sort()
	outF = open("summarized_uses.csv", 'w')
	print >> outF, "not yet implemented"
	outF.close()

def dumpNewDictionary(staticReadPoints, dyn_read, jarDefaults):
	out = open('consolidated.dict', 'w')
	readPoints,falsePos = mergeStatAndDynData(staticReadPoints,dyn_read, jarDefaults)
	usedBySubstitution = findSubstitutionUses( map( lambda x: x[0], jarDefaults.values()))
	readPoints = sorted(readPoints) #implicitly converting from map to list
	readPoints.extend(usedBySubstitution)
	for optName in readPoints:
		print >>out,optName + "\t"
	out.close()

def dumpHTML(staticReadPoints,writePoints, jarDefaults, jarVersion, codeDefaults, outName, dyn_read):	
	"""Takes a giant pile of state, dumps it all as HTML.
	readPoints - map from option name to a list of read points in the code
	writePoints - same for writes
	jarDefaults map from opt name to (description, default value, file)
	jarVersion a string for this hadoop version
	codeDefaults map from opt name to default in code
	outName name for output file
	dyn_read map from option to list of daemons that read it dynamically
"""
	readPoints,falsePos = mergeStatAndDynData(staticReadPoints,dyn_read, jarDefaults)

	outF = open(outName, 'w')
	print >>outF,"""
<html>
<head><title>Hadoop option usage</title>
  <script src="sorttable.js"></script>
</head>
<body>"""

	print >> outF, "<p>Table below describes where each configuration option is read. <br>	"
	print >> outF,"Analysis conducted at",time.ctime(),"on:"
	print >> outF,"<pre>",jarVersion,"</pre></p>"
	print >> outF, "<p>Total of",len(readPoints),"options found."
	print >>outF, len(jarDefaults),"have values in *-defaults.xml.</p>"
	
	print >>outF,"""<p>Notes:
<ul>
<li> Option names starting with <code>CONF</code> are Hadoop config options.
<code>PROP</code> indicates a Java system property.</li>
<li>Italicised options are not mentioned in defaults and not ever written to by code.</li>
<li><code>org.apache.hadoop</code> summarized as <code>o.a.h</code>.</li>
<li><code>TT_Child</code> is the Task runner process.</li>
</ul></p>
<hr/>
<table class="sortable" border="1" width = "1000">
<tr><th>Option name</th><th>Description</th><th>Default</th><th>In</th><th>Daemon</th><th>Position</th></tr>
"""
	undocumentedCount = 0
	for optName in sorted(readPoints):
		useList = readPoints[optName]
		useList.sort()
		uses = str(len(useList))
		if optName not in jarDefaults:
			(defaultVal, desc,defaultFile) = ("", "", "")
			if optName in writePoints:
				undocumented = False
				desc = "<i>Set internally by " + writePoints[optName][0] + "</i>"
			else:
				undocumented = optName.startswith("CONF-")
		else:
			undocumented = False
			(defaultVal, desc, defaultFile) =  jarDefaults[optName]
		
		if optName in codeDefaults:
			defaultVal = defaultVal + " ('"+codeDefaults[optName] + "' is default in code)"

		defaultVal = defaultVal.replace(","," ")
		
		if undocumented:
			undocumentedCount += 1
			print >> outF, "<tr><td rowspan=" + uses+ "><i>" + optName + "</i></td>"
		else:
			print >> outF, "<tr><td rowspan=" + uses+ ">" + optName + "</td>"

		print >> outF, "<td rowspan="+ uses + ">" + desc +  "</td>"
		print >> outF, "<td rowspan="+ uses+ ">"+ defaultVal + "</td>"
		print >> outF, "<td rowspan="+ uses + ">" + defaultFile +  "</td>"

			#first option in useList is special, because we only print name and default and source once
		firstOpt = useList.pop()
		d_p = firstOpt.split(" ") #daemon and position
		print >> outF, getShadedPos(optName, d_p, dyn_read),"</tr>"

		for use in useList:
			d_p = use.split(" ") #daemon and position
			print >> outF, "<tr>",getShadedPos(optName, d_p, dyn_read),"</tr>"
			
	print >>outF, "</table>"
	
	unusedSet = findUnusedOpts(readPoints,jarDefaults)
	print >> outF, "<p>Total of",undocumentedCount,"undocumented options.</p>"

	print len(unusedSet),"possibly unused options."  #to console, not file

	if len(unusedSet) > 0:
		print >>outF, "<hr/><h3>Total of",len(unusedSet),"possibly unused options</h3>"
		print >>outF, "<ul>"
		for o in sorted(unusedSet):
			print >> outF, "<li>"+o+"</li>"
		print >> outF,"</ul>"
	if len(falsePos) > 0:
		print >>outF, "<hr/><h3>Total of",len(falsePos),"real options missed by static analysis</h3>"
		print >>outF, "<p>These are real options that were missed by static analysis but found dynamically</p>"
		print >>outF, "<ul>"
		for o in sorted(falsePos):
			print >> outF, "<li>"+o+"</li>"
		print >>outF, "</ul>"

	
	print >>outF, "</body></html>"
	outF.close()


def getShadedPos(opt, d_p, dyn_read):
	(daemon,pos) = d_p
	
	if opt in dyn_read and daemon in dyn_read[opt]:
		return '<td bgcolor="#DDDDFF">' + daemon + '</td><td>' + pos + "</td>"
	else: 
		return "<td>" + daemon + "</td><td>" + pos + "</td>"

if __name__ == "__main__":
	main()