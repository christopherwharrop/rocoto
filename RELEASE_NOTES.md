# Release Notes

##New for Version 1.2.3

* Add RELEASE_NOTES document
* Numerous minor bug fixes
* Update version of external libraries
  * libxml2 
  * libxml-ruby
  * open4
  * sqlite3
  * sqlite3-ruby
* Add support for Ruby versions > 1.8
* Fix passing of environment variables for Moab and SLURM batch systems
* Fix rocotostat to report only tasks that are valid for the given cycle
* Add basic support for the Cobalt batch system used on ACLF's BlueGene systems
* Add support for lsfcray batch system for use on Cray systems using LSF
* Add support for shared and exclusive use of nodes for LSF
  * &lt;shared&gt;
  * &lt;exclusive&gt;
* Decouple workflow locking from the workflow database
* Add automatic daily Rocoto log rotation and purging
  * Log keep time is configurable in ~/.rocoto/rocotorc with MaxLogDays
* Add automatic database trimming for realtime workflows to enhance performance
  * Can be turned off in ~/.rocoto/rocotorc with AutoVacuum set to false
  * Keep time for old records is configurable in ~/.rocoto/rocotorc with VacuumPurgeDays
* Replace appending of task stderr/stdout output files with automated rolling of output files.
