# (c) Copyright 2011 the Regents of the University of California
# Portions (c) Copyright 2011 Cloudera, Inc.
#  See the file COPYING for license information

import glob
import os
import re
import sys
import time

OUTFILE = "dynamic_results.html"

def main():
    if len(sys.argv) < 2:
        print "usage: summarize-dynlogs <list of log files>"
        sys.exit(1)
    
    opt_to_process_list = {} #maps option name to set of processes that use it at least once
    opt_proc_to_scen_count = {} #maps an option-process pair (as string pair with - separator)
                #to a list of scenario-count-source tuples.
    all_procs = set([])
    for logfile in sys.argv[1:]:
        print logfile
        scan(logfile, opt_to_process_list,opt_proc_to_scen_count,all_procs)
        
    print "saw a total of",len(opt_to_process_list),"options, split across",len(all_procs),"processes"    
    dump_HTML(opt_to_process_list,opt_proc_to_scen_count,all_procs)
    dump_opts_by_daemon(opt_to_process_list,"daemon_summary.out")


NAME_REGEX = re.compile("(.*)-([^-]*).log")
READ_REGEX_CDH = re.compile("Config Monitoring: ([^ ]+) at .* from (.*)")
READ_REGEX_VANILLA= re.compile("Config Monitoring: ([^ ]+) at .*")

READ_REGEX = READ_REGEX_VANILLA


def scan(logfilename, opt_to_process_list,opt_proc_to_scen_count, all_procs):
    match = NAME_REGEX.match(logfilename)
    if not match:
        print "all input logs should be named as scenario-daemon.log"
        sys.exit(1)
    
    (scenario, process) = match.group(1), match.group(2)
    print "scenario",scenario,"process",process
    all_procs.add(process)

    f = open(logfilename, 'r')
    read_in_log = {} #map from option name to read count, for this logfile
    opt_sources = {} #opt name to source
    for ln in f:
        opt_read = READ_REGEX.search(ln)
        if opt_read:
            opt_name = opt_read.group(1)
            opt_source = opt_read.group(2) if opt_read.lastindex > 1 else ""
    
            if opt_source.endswith("/job.xml"):
                opt_source = "job.xml"
            
            read_in_log[opt_name] = read_in_log.get(opt_name,0) + 1
            if opt_name in opt_sources:
                if not opt_source in opt_sources[opt_name]:
                    print "WARN:",opt_name,"appears with inconsistent sources in same scenario:", \
                         opt_source,"and",opt_sources[opt_name]
                    opt_sources[opt_name] = opt_source + " or " + opt_sources[opt_name]
            else:
                opt_sources[opt_name] = opt_source
            
#            print "found a match for",opt_name
    f.close()
    print "total of",len(read_in_log),"distinct options in this file"
    
    
    for opt in read_in_log:
        if opt in opt_to_process_list:
            opt_to_process_list[opt].add(process)
        else:
            opt_to_process_list[opt] = set([process])
        opair = opt+'-'+process
        if opair in opt_proc_to_scen_count:
            opt_proc_to_scen_count[opair].append(  (scenario, read_in_log[opt], opt_sources[opt]) )
        else:
            opt_proc_to_scen_count[opair] = [ (scenario, read_in_log[opt],opt_sources[opt])] 

    return

def dump_HTML(opt_to_process_list,opt_proc_to_scen_count,all_procs_set):
    all_procs = []
    all_procs.extend(all_procs_set)
    all_procs.sort()
    out = open(OUTFILE,'w')
    print >>out,"""<html><head><title>Options read dynamically</title></head>
<body>     
"""
    print >>out,"<p>Saw a total of",len(opt_to_process_list),"options, split across",len(all_procs),"processes.</p>"

    print >>out,"Generated at",time.ctime()
    print >>out,'<table class="sortable" border="1" width = "1000"><tr><th>Option</th>'
    for p in all_procs:
        print  >>out,"<th>",p,"</th>"
    print  >>out,"</tr>"
    for (option,plist) in sorted(opt_to_process_list.items()):
        print  >>out, "<tr><td>",option,"</td>"
        for proc in all_procs:
            if proc in plist:
                scenlist = opt_proc_to_scen_count[option+'-'+proc]
                print >>out,'<td bgcolor="#DDDDFF">'

                for (scen,count,source) in scenlist:
                     print >>out,scen,count,source,"<br>"
                print >>out,"</td>"
            else:
                print >>out,'<td bgcolor="#FFFFDD">-</td>'
    print >>out,"</table>"
    print >>out,"</body></html>"
    out.close()
    return

def dump_opts_by_daemon(opt_to_process_list,out_file):
    out = open(out_file,'w')

    for (option,plist) in sorted(opt_to_process_list.items()):
        cnames = canonical_names(plist)
        print  >>out,option,",".join(cnames)
    out.close()
    return

def canonical_names(plist):
    """Takes a list of short process names, returns a set of canonical process names"""
    canonical = set([])
    for o in plist:
        if o == "dn":
            canonical.add('DataNode')
        elif o == "nn" or o == "nn_format":
            canonical.add('NameNode')
        elif o == "2nn":
            canonical.add('SecondaryNN')
        elif o == "put" or o =="client":
            canonical.add('FS_Shell')            

        elif o == "tt":
            canonical.add('TaskTracker')
        elif o == "jt":
            canonical.add('JobTracker')
        elif o == "userlogs":
            canonical.add('TT_Child')
        elif o == "submit":
            canonical.add('JobClient')
        else:
            print "No idea what to do with daemon named",o
    return canonical
if __name__ == '__main__':
    main()
