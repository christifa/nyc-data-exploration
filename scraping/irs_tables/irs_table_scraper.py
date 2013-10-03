#!/usr/bin/env python
# encoding: utf-8
'''
irs_table_scraper -- scrapes IRS data from www.melissadata.com for given zips

irs_table_scraper is a python script that reads in a list of zips and scrapes the 
IRS data for those scrips from www.melissadata.com

@author:     Gregory Kramida
        
@copyright:  2013 Gregory Kramida and Jonathan Gluck. All rights reserved.
        
@license:    Apache License 2.0

@contact:    algomorph@gmail.com
@deffield    updated: Updated
'''

import sys
import os
import re
import tablescrape as tsc
import numpy as np
import time

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

__all__ = []
__version__ = 0.1
__date__ = '2013-10-03'
__updated__ = '2013-10-03'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg
    

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''
    
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by algomorph on %s.
  Copyright 2013 Gregory Kramida and Jonathan Gluck. All rights reserved.
  
  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0
  
  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))
    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose",default=0,type=int, dest="verbose", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('-z', "--zip_codes",default="zip_codes.csv",dest="zip_codes", help="Path to the file containing 5-digit zip codes in a single column.")
        parser.add_argument('-n', "--num_zips",type=int,default=500000, help="Total number of zip code areas to process")
        parser.add_argument('-o', "--output",default="zip_income.csv", help="Path to the output csv file. Existing file will be overwritten.")
        parser.add_argument('-e', "--error_file",default="errors.csv", help="Path to the error csv file. Existing file will be overwritten.")
        # Process arguments
        args = parser.parse_args()
        
        verbose = args.verbose
        zip_path = args.zip_codes
        out_path = args.output
        err_path = args.error_file
        
        if verbose > 0:
            print("Verbose mode on")
        
        if(not os.path.isfile(zip_path)):
            raise CLIError("Could not find text file at " % zip_path)
        #read zips
        f = open(zip_path);
        zips = sorted(f.readlines());
        f.close()
        #find proper upper bound
        n_zips = min(len(zips),args.num_zips)
        #prepare data structure for output
        n_years = 11
        first_year = 2000
        out_arr = np.empty((n_zips*n_years,19),dtype=np.int32)
        tcell_regex = re.compile('td|th')
        cell_format_regex = re.compile('%|\$|,')
        i_out_row = 0
        errors = []
        for i_zip in xrange(0,n_zips):
            szip = zips[i_zip].rstrip()
            if(verbose > 0):
                print "Processing zip %s, %d of %d" % (szip, i_zip, n_zips)
            url = "http://www.melissadata.com/lookups/TaxZip.asp?Zip="+szip+"&submit1=Submit"
            soup = tsc.opensoup(url)
            tables = soup.findAll("table")
            table = tables[5]
            #skip first two rows - table header and year column headers
            #also skip last row - footer
            rows = table.findAll('tr')
            if(len(rows) != 21):
                print len(rows)
                if verbose > 0:
                    print "Missing data for zip %s" % szip
                continue
            rows = rows[2:19]
            
            #prep the first 10 rows
            out_arr[i_out_row:i_out_row+n_years,0] = [int(szip)]*n_years #zip goes in first col
            out_arr[i_out_row:i_out_row+n_years,1] = range(first_year,first_year + n_years) #years go in second col
            i_out_col = 2
            
            for row in rows:
                #skip first cell (row header)
                cells = row.findAll(tcell_regex)[1:]
                i_year = 0
                for cell in cells:
                    #strip tags, $, %, and remove commas
                    str_cell = cell_format_regex.sub("",tsc.striptags(cell.renderContents()))
                    
                    if(str_cell != "N/A"):
                        try:
                            out_arr[i_out_row+i_year,i_out_col] = int(str_cell)
                        except ValueError, ve:
                            out_arr[i_out_row+i_year,i_out_col] = -1000000
                            errors.append([int(szip), first_year + i_year, i_out_col])
                            pass #don't
                    else:
                        out_arr[i_out_row+i_year,i_out_col] = -999999
                    
                    i_year +=1 
                i_out_col+=1
            i_out_row += n_years
            time.sleep(1)
            
        #store output
        np.savetxt(out_path, out_arr, delimiter = ",", fmt='%d')
        np.savetxt(err_path, np.asarray(errors,dtype=np.int32), delimiter = ",", fmt='%d')
        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    '''
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + " for help use --help")
        return 2
    '''
    
    
    

if __name__ == "__main__":
    
    if DEBUG:
        sys.argv.append("-v=1")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'irs_table_scraper_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())
