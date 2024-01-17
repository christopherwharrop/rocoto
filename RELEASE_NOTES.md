# Release Notes

## New for Version 1.3.6

* Fix typo in Slurm support causing persistent UNAVAILABLE state
* Replace deprecated File.exists?() with File.exist?()

## New for Version 1.3.5

* Remove automatic detection of default nodesize from PBSPro support.
* Deprecate use of <nodesize> tag.
* Deprecate support for <cores> tag for PBSPro.
* Remove unused/dead code from PBSPro support.

## New for Version 1.3.4

* Fix INSTALL script incompatibilities with Bourne shell.
* Fix missing sqlite3 library path during installation.

## New for Version 1.3.3

* Performance improvements
* Store configuration in ~/.rocoto/$VERSION/rocotorc instead of ~/.rocoto/rocotorc to allow use of multiple versions of Rocoto.
* Add capability to control batch system command timeouts from ~/.rocoto/$VERSION/rocotorc file.
* Store logs in ~/.rocoto/$VERSION/$WORKFLOW_ID/log instead of ~/.rocoto/log to improve logging
* Increase internal inter-server timeouts to increase resiliency on systems under heavy loads.
* Fix bugs and deprecation warnings when using Ruby 2.7.x
* Add support for the <exclusive> and <shared> tags to Slurm
* Fix bug when using command line options to select all tasks

## New for Version 1.3.2

* Fix bug in Slurm batch system interface that caused UNAVAILABLE states to persist forever.

## New for Version 1.3.1

* Fix XML validation bug that caused sensitivity to ordering of tags
* Fix <nodes> handling for PBSPro to allow specificaiton of node features 

## New for Version 1.3.0

* Update SLURM support to handle pack groups
* Update SLURM support to map `<queue>` to --qos instead of --partition
* Update SLURM support to map `<partition>` to --partition
* Update LSF support to handle additional methods LSF uses to report the exit status
* All rocoto commands have the same -a, -c, -m, and -t options
* The -c and -t options can now select by cycledefs and attributes (ie. final)
* The new "rocotocomplete" command can mark tasks or cycles as having completed
* Manpages are updated to reflect current capabilities

## New for Version 1.2.4

* Fix bugs relating to ~/.rocoto/log rotation
* Fix race condition bug relating to ~/.rocoto/rocotorc configuration file
* Increase reliability and performance of database and workflow locking
* Fix bug in Rocoto commands related to sourcing of shell init script
* Improve performance and reduce system load when monitoring for orphaned Rocoto processes
* Fix bug in XML validation that erroneously enforced ordering of metatask contents
* Fix bug in processing of XML special characters
* Fix task list update bug in rocotorewind
* Fix bug in rotocoboot that prevent task output files from being rotated
* Fix bugs in Cobalt batch system interface that were exposed in Ruby > 1.8.7
* Fix bug in LSF batch system interface related to long job names
* Add support for PBSPro batch system (e.g. for use on Cheyenne)
* Update libxml-ruby to version 3.0.0 to mitigate memory management bugs
* Add new cyclestring flags
  * @n = Number of days in the month
  * @o = All lower case abbreviated month name
  * @O = All lower case full month name
* Add new &lt;taskvalid&gt; dependency to trigger tasks based on whether or not a particular task is valid for the current cycle time
