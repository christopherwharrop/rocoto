##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowLockedException
  #
  ##########################################
  class WorkflowLockedException < RuntimeError
  end

  ##########################################
  #
  # Class WorkflowDBLockedException
  #
  ##########################################
  class WorkflowDBLockedException < RuntimeError
  end

  ##########################################
  #
  # Class WorkflowSQLite3DB
  #
  ##########################################
  class WorkflowSQLite3DB

    require "sqlite3"
    require "socket"
    require "workflowmgr/cycle"
    require "workflowmgr/job"
    require "workflowmgr/workflowoption"
    require "workflowmgr/workflowconfig"
    require "workflowmgr/utilities"

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file)

      # Set name of database file
      @database_file=database_file

      # Calculate the name of the corresponding workflow lock database
      @database_lock_file = if @database_file =~ /^(\S+)(\.[^\.]+)$/
        "#{$1}_lock#{$2}"
      else
        "#{@database_file}_lock"
      end

      writable = ( not File.exist?(@database_file)             \
                   or File.writable?(@database_file) ) and     \
                 ( not File.exist?(@database_lock_file)        \
                   or File.writable?(@database_lock_file) )
      @readonly=!writable

    end


    def readonly?
      return @readonly
    end

    ##########################################
    #
    # dbopen
    #
    ##########################################
    def dbopen

      # Open the lock database
      open_lock_db()

    end  # dbopen


    ##########################################
    #
    # lock_workflow
    #
    ##########################################
    def lock_workflow

      if readonly?
        open_workflow_db()
        return true
      end

      begin

        # Start a transaction and immediately acquire an exclusive lock
        @database_lock.transaction(mode=:exclusive) do |db|

          # Access the workflow lock maintained by the workflow manager
          lock=db.execute("SELECT * FROM lock;")      

          # If no lock is present, we have acquired the lock.  Write the WFM's pid and host into the lock table
          if lock.empty?
            db.execute("INSERT INTO lock VALUES (#{Process.pid},'#{Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]}',#{Time.now.to_i});") 
 
          # Otherwise, we didn't get the lock, but we need to check to make sure the lock is not stale
          else

            stale=false

            # If we are on the same host as the owner of the lock, run a simple local check to see if the owner is still running
            if Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]==lock[0][1]
              begin
                Process.getpgid(lock[0][0])
              rescue Errno::ESRCH
                stale=true
              end

            # Otherwise do a kill -0 through an ssh tunnel if the lock is 5 minutes old ## BUG, make this configurable
            else
              if Time.now - Time.at(lock[0][2]) > 300
                begin
                  WorkflowMgr.timeout(10) do
                    system("ssh -o StrictHostKeyChecking=no #{lock[0][1]} kill -0 #{lock[0][0]} 2>&1 > /dev/null")
                    stale=$?.exitstatus!=0
                  end
                rescue Timeout::Error
                  stale=true
                end
              end
            end

            # If the lock is stale, steal the lock
            localhostinfo=Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0]
            lockhostinfo=Socket::getaddrinfo(lock[0][1],nil)[0]
            if stale
              db.execute("DELETE FROM lock;")
              db.execute("INSERT INTO lock VALUES (#{Process.pid},'#{localhostinfo[3]}',#{Time.now.to_i});")
              msg="WARNING: Rocoto pid #{Process.pid} on host #{localhostinfo[2]} (#{localhostinfo[3]}) stole stale lock from Rocoto pid #{lock[0][0]} on host #{lockhostinfo[2]} (#{lockhostinfo[3]})."
              WorkflowMgr.stderr(msg,3)
              WorkflowMgr.log(msg)
            else
              msg="WARNING: Workflow is locked by pid #{lock[0][0]} on host #{lockhostinfo[2]} (#{lockhostinfo[3]}) since #{Time.at(lock[0][2])}."
              raise WorkflowMgr::WorkflowLockedException, msg
            end
          end

        end  # database transaction

        # If an exception wasn't thrown, we got the lock
        # So, open the workflow database and, if that 
        # doesn't throw an exception, return true
        open_workflow_db()
        return true

      rescue WorkflowMgr::WorkflowLockedException
        WorkflowMgr.stderr("#{$!}",3)
        WorkflowMgr.log("#{$!}")
        return false
      rescue SQLite3::BusyException
        msg="WARNING: WorkflowSQLite3DB.lock_workflow: Could not open workflow database file '#{@database_lock_file}' because it is locked by SQLite."
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # unlock_workflow
    #
    ##########################################
    def unlock_workflow

      return true if readonly?

      begin

        # Start a transaction so that the database will be locked
        @database_lock.transaction do |db|

          lock=db.execute("SELECT * FROM lock;")      

          if Process.pid==lock[0][0] && Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]==lock[0][1]
            db.execute("DELETE FROM lock;")      
          else
            msg="ERROR: Process #{Process.pid} cannot unlock the workflow because it is locked locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}."
            raise WorkflowMgr::WorkflowLockedException, msg
          end

        end  # database transaction

      rescue WorkflowMgr::WorkflowLockedException
        WorkflowMgr.stderr("#{$!}",3)
        WorkflowMgr.log("#{$!}")
        Process.exit(1)
      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.unlock_workflow: Could not open workflow database file '#{@database_lock_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # get_cycledefs
    #
    ##########################################
    def get_cycledefs

      begin

        dbcycledefs=[]

        # Retrieve the cycle definitions from the database
        dbcycledefs=@database.execute("SELECT groupname,activation_offset,cycledef,dirty FROM cycledef;")

        # Return the array of cycledefs
        dbcycledefs.collect! do |cycledef| 
          if cycledef[3].nil?
            {:group=>cycledef[0], :activation_offset=>(cycledef[1] || 0), :cycledef=>cycledef[2], :position=>nil} 
          else
            {:group=>cycledef[0], :activation_offset=>(cycledef[1] || 0), :cycledef=>cycledef[2], :position=>Time.at(cycledef[3]).getgm} 
          end
        end  # 

        return dbcycledefs

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_cycledefs: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # set_cycledefs
    #
    ##########################################
    def set_cycledefs(cycledefs)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Delete all current cycledefs from the database
          dbspecs=db.execute("DELETE FROM cycledef;")

          @database.prepare("INSERT INTO cycledef VALUES (NULL,?,?,?,?)") do |stmt|
 
            # Add new cycledefs to the database
            cycledefs.each do |cycledef|
              if cycledef[:position].nil?
                stmt.execute("#{cycledef[:group]}","#{cycledef[:cycledef]}",nil,cycledef[:activation_offset])
              else
                stmt.execute("#{cycledef[:group]}","#{cycledef[:cycledef]}",cycledef[:position].to_i,cycledef[:activation_offset])
              end
            end

          end  # prepare

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.set_cycledefs: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin
  
    end


    ##########################################
    #
    # get_cycle
    #
    ##########################################
    def get_cycle(reftime=Time.gm(1900,1,1,0,0))

      begin

        dbcycles=[]

        # Retrieve all cycles from the cycle table
        dbcycles=@database.execute("SELECT cycle,activated,expired,done,draining FROM cycles WHERE cycle == #{reftime.getgm.to_i};")

        # Return an array of cycles
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm,{:activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm, :draining=>Time.at(cycle[4] || 0).getgm }) }

        return dbcycles

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_cycle: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end

    ##########################################
    #
    # get_cycles
    #
    ##########################################
    def get_cycles(reftime={ :start=>Time.gm(1900,1,1,0,0), :end=>Time.gm(9999,12,31,23,59) } )

      begin

        dbcycles=[]

        # Get the starting and ending cycle times
        startcycle=reftime[:start].nil? ? startcycle=Time.gm(1900,1,1,0,0) : reftime[:start].getgm
        endcycle=reftime[:end].nil? ? endcycle=Time.gm(9999,12,31,23,59) : reftime[:end].getgm+1 

        # Retrieve all cycles from the cycle table
        dbcycles=@database.execute("SELECT cycle,activated,expired,done,draining FROM cycles WHERE cycle >= #{startcycle.getgm.to_i} and cycle <= #{endcycle.getgm.to_i};")

        # Return an array of cycles
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm, { :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm, :draining=>Time.at(cycle[4] || 0).getgm }) }

        return dbcycles

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_cycles: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # get_last_cycle
    #
    ##########################################
    def get_last_cycle

      begin

        dbcycles=[]

        # Get the maximum cycle time from the database
        max_cycle=@database.execute("SELECT MAX(cycle) FROM cycles")[0][0]
        unless max_cycle.nil?
          dbcycles=@database.execute("SELECT cycle,activated,expired,done,draining FROM cycles WHERE cycle=#{max_cycle}")
        end
          
        # Return the last cycle
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm, { :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm, :draining=>Time.at(cycle[4] || 0).getgm }) }

        return dbcycles.first

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_last_cycle: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end

    ##########################################
    #
    # get_active_cycles
    #
    ##########################################
    def get_active_cycles

      begin

        dbcycles=[]

        # Get the cycles that are neither done nor expired
        dbcycles=@database.execute("SELECT cycle,activated,expired,done,draining FROM cycles WHERE done=0 and expired=0;")

        # Return the array of cycle specs
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm, { :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm, :draining=>Time.at(cycle[4] || 0).getgm}) }

        return dbcycles

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_active_cycles: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                        

    end

    ##########################################
    #
    # update_cycles
    #
    ##########################################
    def update_cycles(cycles)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Update each cycle in the database
          cycles.each { |newcycle|
            db.execute("UPDATE cycles SET activated=#{newcycle.activated.to_i},expired=#{newcycle.expired.to_i},done=#{newcycle.done.to_i},draining=#{newcycle.draining.to_i} WHERE cycle=#{newcycle.cycle.to_i};")
          }

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.update_cycles: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end


    ##########################################
    #
    # add_cycles
    #
    ##########################################
    def add_cycles(cycles)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Add each cycle to the database
          cycles.each { |newcycle|
            db.execute("INSERT INTO cycles VALUES (NULL,#{newcycle.cycle.to_i},#{newcycle.activated.to_i},#{newcycle.expired.to_i},#{newcycle.done.to_i},#{newcycle.draining.to_i});")
          }

        end  # database transaction
          
      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.add_cycles: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end


    ##########################################
    #
    # get_jobs
    #
    ##########################################
    def get_jobs(cycles=nil)

      begin

        jobs={}
        dbjobs=[]

        # Start a transaction so that the database will be locked
        @database.results_as_hash = true
        if cycles.nil?

          # Retrieve all jobs from the job table
          dbjobs=@database.execute("SELECT * FROM jobs;")

        else

          # Retrieve all jobs from the job table that match the cycles provided
          cycles.each do |cycle|
            dbjobs+=@database.execute("SELECT * FROM jobs WHERE cycle = #{cycle.to_i};")
          end

        end        

        # jobid,taskname,cycle,cores,state,native_state,exit_status,tries,nunknowns,duration
        dbjobs.each do |job|
          task=job['taskname']
          cycle=Time.at(job['cycle']).getgm
          jobs[task]={} if jobs[task].nil?
          jobs[task][cycle] = Job.new(job['jobid'],             # jobid
                                      task,                     # taskname
                                      cycle,                    # cycle
                                      job['cores'].to_i,        # cores
                                      job['state'],             # state
                                      job['native_state'],      # native state
                                      job['exit_status'].to_i,  # exit_status
                                      job['tries'].to_i,        # tries
                                      job['nunknowns'].to_i,    # nunknowns
                                      job['duration'].to_f      # duration
                                     )

        end

        # Return jobs hash
        return jobs

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_jobs: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      ensure
        @database.results_as_hash = false
      end  # begin                                                                                                                                                                                                                                                         

    end


    ##########################################
    #
    # add_jobs
    #
    ##########################################
    def add_jobs(jobs)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Add or update each job in the database
          jobs.each do |job|
            db.execute("INSERT INTO jobs VALUES (NULL,'#{job.id}','#{job.task}',#{job.cycle.to_i},#{job.cores},'#{job.state}','#{job.native_state}',#{job.exit_status},#{job.tries},#{job.nunknowns},#{job.duration});")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.add_jobs: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end


    ##########################################
    #
    # update_jobs
    #
    ##########################################
    def update_jobs(jobs)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Add or update each job in the database
          jobs.each do |job|
            db.execute("UPDATE jobs SET jobid='#{job.id}',state='#{job.state}',native_state='#{job.native_state}',exit_status=#{job.exit_status},tries=#{job.tries},nunknowns=#{job.nunknowns},duration=#{job.duration} WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.update_jobs: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end

    ##########################################
    #
    # update_jobids
    #
    ##########################################
    def update_jobids(jobs)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Add or update each job in the database
          jobs.each do |job|
            db.execute("UPDATE jobs SET jobid='#{job.jobid}' WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.update_jobids: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end


    ##########################################
    #
    # delete_jobs
    #
    ##########################################
    def delete_jobs(jobs)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Delete each job from the database
          jobs.each do |job|
            db.execute("DELETE FROM jobs WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.delete_jobs: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end


    ##########################################
    #
    # get_bqservers
    #
    ##########################################
    def get_bqservers

      begin

        dbbqservers=[]

        dbbqservers=@database.execute("SELECT uri FROM bqservers;")

        # Return jobs hash
        return dbbqservers.flatten

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_bqservers: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end


    ##########################################
    #
    # add_bqservers
    #
    ##########################################
    def add_bqservers(bqservers)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Add or update each bqserver in the database
          bqservers.each do |bqserver|
            db.execute("INSERT INTO bqservers VALUES (NULL,'#{bqserver}');")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.add_bqservers: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         

    end

    ##########################################
    #
    # delete_bqservers
    #
    ##########################################
    def delete_bqservers(bqservers)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Delete each job from the database
          bqservers.each do |bqserver|
            db.execute("DELETE FROM bqservers WHERE uri='#{bqserver}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.delete_bqservers: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end


    ##########################################
    #
    # get_downpaths
    #
    ##########################################
    def get_downpaths

      begin

        dbdownpaths=[]

        # Retrieve all downpaths from the job table
        dbdownpaths=@database.execute("SELECT path,downdate,host,pid FROM downpaths;")

        # Return an array of downpaths
        dbdownpaths.collect! { |downpath| {:path=>downpath[0], :downtime=>Time.at(downpath[1]).getgm, :host=>downpath[2], :pid=>downpath[3]} }

        # Return downpaths hash
        return dbdownpaths

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_downpaths: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end


    ##########################################
    #
    # add_downpaths
    #
    ##########################################
    def add_downpaths(downpaths)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Add or update each job in the database
          downpaths.each do |downpath|
            db.execute("INSERT INTO downpaths VALUES (NULL,'#{downpath[:path]}',#{downpath[:downtime].to_i},'#{downpath[:host]}',#{downpath[:pid]});")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.add_downpaths: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end


    ##########################################
    #
    # delete_downpaths
    #
    ##########################################
    def delete_downpaths(downpaths)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Delete each downpath from the database
          downpaths.each do |downpath|
            db.execute("DELETE FROM downpaths WHERE path='#{downpath[:path]}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.delete_downpaths: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end


    ##########################################
    #
    # get_vacuum_time
    #
    ##########################################
    def get_vacuum_time

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Access the vacuum table
          vacuum=db.execute("SELECT * FROM vacuum;")

          # If no vacuum time is present, we have never vacuumed
          if vacuum.empty?
            return Time.at(0).getgm
          else
          # Return the last vacuum time
            return Time.at(vacuum[0][0]).getgm
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_vacuum_time: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # set_vacuum_time
    #
    ##########################################
    def set_vacuum_time(vacuum_time)

      begin

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Remove the old vacuum time
          db.execute("DELETE FROM vacuum;")
          db.execute("INSERT INTO vacuum VALUES (#{vacuum_time.to_i});")

        end  # database transaction

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.set_vacuum_time: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # vacuum
    #
    ##########################################
    def vacuum(age)

      vacuum_date = Time.now.to_i - age

      begin

        # Count the number of jobs removed
        njobs_removed = 0

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Remove jobs from all old expired cycles
          db.execute("DELETE from jobs where jobs.id in (SELECT jobs.id from jobs INNER JOIN cycles ON jobs.cycle = cycles.cycle where (cycles.expired < #{vacuum_date} and cycles.expired > 0));")

          # Get the number of jobs removed from expired cycles
          njobs_removed += db.changes

          # Remove jobs from all old completed cycles
          db.execute("DELETE from jobs where jobs.id in (SELECT jobs.id from jobs INNER JOIN cycles ON jobs.cycle = cycles.cycle where (cycles.done < #{vacuum_date} and cycles.done > 0));")

          # Get the number of jobs removed from completed cycles
          njobs_removed += db.changes

        end  # database transaction

        # Recover the empty space
        @database.execute("VACUUM;")

        WorkflowMgr.stderr("Vacuumed database. Removed #{njobs_removed} jobs",3)
        WorkflowMgr.log("Vacuumed database. Removed #{njobs_removed} jobs")

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.vacuum: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin

    end


    ##########################################
    #
    # get_tables
    #
    ##########################################
    def get_tables(tablelist=nil)

      begin

        tables={}

        # Get a listing of the database tables
        dbtables = @database.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        dbtables.flatten!

        # Create an array of rows for each table
        dbtables.each do |table|

          # Initialize the array of rows to be empty
          tables[table.to_sym]=[]

          # Get all rows for the table, where first row is column names
          dbtable=@database.execute2("SELECT * FROM #{table};")

          # Get the table column names
          columns=dbtable.shift

          # Add each table row to the array of rows for this table
          dbtable.each do |row|
            rowdata={}

            # Loop over columns, creating a hash for this row's data
            columns.each_with_index do |column,idx|
              rowdata[column.to_sym]=row[idx]
            end

            # Add the hash representing this row to the array of rows for this table
            tables[table.to_sym] << rowdata

          end 

        end  # dbtables.each

        # Return the tables
        tables

      rescue SQLite3::BusyException
        msg="ERROR: WorkflowSQLite3DB.get_tables: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg
      end  # begin                                                                                                                                                                                                                                                         
    end


  private


    ##########################################
    #
    # open_lock_db
    #
    ##########################################
    def open_lock_db

      # Cannot use the lock database in read-only mode.
      return true if readonly?

      begin
        # Get a handle to the lock  database
        if readonly?
          @database_lock = SQLite3::Database.new(@database_lock_file,{:readonly=>true})
        else
          @database_lock = SQLite3::Database.new(@database_lock_file)
        end

        # Set the retry limit (milliseconds) for locked resources
        @database_lock.busy_timeout=10000

        # Return results as arrays
        @database_lock.results_as_hash=false

        # Start a transaction so that the database will be locked
        @database_lock.transaction do |db|

          # Get a listing of the database tables
          tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

          # Create the lock table if it doesn't exist
          unless tables.flatten.member?("lock")
            db.execute("CREATE TABLE lock (pid INTEGER, host VARCHAR(64), time DATETIME);")
          end

        end  # database transaction

      rescue SQLite3::BusyException
        msg="WorkflowSQLite3DB.open_lock_db: Could not open workflow database file '#{@database_lock_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg

      end  # begin


    end


    ##########################################
    #
    # open_workflow_db
    #
    ##########################################
    def open_workflow_db

      begin
        # Get a handle to the database
        if readonly?
          WorkflowMgr.stderr('Opening database in read-only mode.',-9e9)
          @database = SQLite3::Database.new(@database_file,{:readonly=>true})
        else
          @database = SQLite3::Database.new(@database_file)
        end

        puts "database = #{@database}"

        # Set the retry limit (milliseconds) for locked resources
        @database.busy_timeout=10000

        # Return results as arrays
        @database.results_as_hash=false

        # Start a transaction so that the database will be locked
        @database.transaction do |db|

          # Get a listing of the database tables
          tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

          # Create all tables that are missing from the database 
          create_tables(db,tables.flatten)

          # Update the tables if needed
          update_tables(db)

        end  # database transaction

      rescue SQLite3::BusyException
        msg="WorkflowSQLite3DB.dbopen: Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
        raise WorkflowMgr::WorkflowDBLockedException,msg

      end  # begin

    end


    ##########################################
    #
    # create_tables
    #
    ##########################################
    def create_tables(db,tables)

      return true if readonly?

      raise "WorkflowSQLite3DB::create_tables must be called inside a transaction" unless db.transaction_active?

      # Create the cycledef table
      unless tables.member?("cycledef")
        db.execute("CREATE TABLE cycledef (id INTEGER PRIMARY KEY, groupname VARCHAR(64), cycledef VARCHAR(256), dirty BOOLEAN);")
      end

      # Create the cycles table
      unless tables.member?("cycles")
        db.execute("CREATE TABLE cycles (id INTEGER PRIMARY KEY, cycle DATETIME, activated DATETIME, expired DATETIME, done DATETIME, draining DATETIME);")
      end

      # Create the jobs table
      unless tables.member?("jobs")
        db.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, jobid VARCHAR(64), taskname VARCHAR(64), cycle DATETIME, cores INTEGER, state VARCHAR[64], native_state VARCHAR[64], exit_status INTEGER, tries INTEGER, nunknowns INTEGER, duration REAL);")
      end
      
      # Create the bqservers table
      unless tables.member?("bqservers")
        db.execute("CREATE TABLE bqservers (id INTEGER PRIMARY KEY, uri VARCHAR(1024));")
      end
      
      # Create the downpaths table
      unless tables.member?("downpaths")
        db.execute("CREATE TABLE downpaths (id INTEGER PRIMARY KEY, path VARCHAR(1024), downdate DATETIME, host VARCHAR[64], pid INTEGER);")
      end

      # Create the vacuum table
      unless tables.member?("vacuum")
        db.execute("CREATE TABLE vacuum (last_vacuum DATETIME);")
      end

    end  # create_tables

    ##########################################
    #
    # update_tables
    #
    ##########################################
    def update_tables(db)

      raise "WorkflowSQLite3DB::update_tables must be called inside a transaction" unless db.transaction_active?

      # Get the command used to create the jobs table
      jobscrt = db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='jobs' AND type='table';").to_s

      # Parse the jobs command to see if the duration column is not there
      unless jobscrt=~/duration REAL/
        db.execute("ALTER TABLE jobs ADD COLUMN duration REAL;")
      end

      # Get the command used to create the cycle table
      cyclescrt = db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='cycles' AND type='table';").to_s

      # Parse the cycle command to see if the draining column is not there
      unless cyclescrt=~/draining DATETIME/
        db.execute("ALTER TABLE cycles ADD COLUMN draining DATETIME;")
      end

      # Get the command used to create the cycle table
      cycledefscrt = db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='cycledef' AND type='table';").to_s

      # Parse the cycle command to see if the draining column is not there
      unless cycledefscrt=~/activation_offset INTEGER/
        db.execute("ALTER TABLE cycledef ADD COLUMN activation_offset INTEGER;")
      end

    end  # update_tables

  end  # Class WorkflowSQLite3DB

end  # Module WorkflowMgr
