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
    require "system_timer"
    require "workflowmgr/cycle"
    require "workflowmgr/job"

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file)

      @database_file=database_file

    end


    ##########################################
    #
    # dbopen
    #
    ##########################################
    def dbopen

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Get a listing of the database tables
          tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

          # Create all tables that are missing from the database 
          create_tables(db,tables.flatten)

        end  # database transaction

      rescue SQLite3::BusyException
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"

      end  # begin

    end  # initialize


    ##########################################
    #
    # lock_workflow
    #
    ##########################################
    def lock_workflow

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Access the workflow lock maintained by the workflow manager
          lock=db.execute("SELECT * FROM lock;")      

          # If no lock is present, we have acquired the lock.  Write the WFM's pid and host into the lock table
          if lock.empty?
            db.execute("INSERT INTO lock VALUES (#{Process.ppid},'#{Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]}',#{Time.now.to_i});") 
 
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
                  SystemTimer.timeout(10) do
                    system("ssh #{lock[0][1]} kill -0 #{lock[0][0]}")
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
              db.execute("INSERT INTO lock VALUES (#{Process.ppid},'#{localhostinfo[3]}',#{Time.now.to_i});")
              STDERR.puts "WARNING: Workflowmgr pid #{Process.ppid} on host #{localhostinfo[2]} (#{localhostinfo[3]}) stole stale lock from Workflowmgr pid #{lock[0][0]} on host #{lockhostinfo[2]} (#{lockhostinfo[3]})."
            else
              raise WorkflowMgr::WorkflowLockedException, "ERROR: Workflow is locked by pid #{lock[0][0]} on host #{lockhostinfo[2]} (#{lockhostinfo[3]}) since #{Time.at(lock[0][2])}"
            end
          end

        end  # database transaction

        # If an exception wasn't thrown, we got the lock
        return true

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        return false
      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}' for lock_workflow"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        return false
      end  # begin

    end


    ##########################################
    #
    # unlock_workflow
    #
    ##########################################
    def unlock_workflow

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          lock=db.execute("SELECT * FROM lock;")      

          if Process.ppid==lock[0][0] && Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3]==lock[0][1]
            db.execute("DELETE FROM lock;")      
          else
            raise WorkflowMgr::WorkflowLockedException, "ERROR: Process #{Process.ppid} cannot unlock the workflow because it does not own the lock." +
                                                          "       The workflow is already locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}."
          end

        end  # database transaction

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        Process.exit(1)
      rescue SQLite3::BusyException
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Access the workflow lock maintained by the workflow manager
          dbcycledefs=db.execute("SELECT groupname,cycledef,dirty FROM cycledef;")

        end  # database transaction
          
        # Return the array of cycledefs
        dbcycledefs.collect! do |cycledef| 
          if cycledef[2].nil?
            {:group=>cycledef[0], :cycledef=>cycledef[1], :position=>nil} 
          else
            {:group=>cycledef[0], :cycledef=>cycledef[1], :position=>Time.at(cycledef[2]).getgm} 
          end
        end  # 

        return dbcycledefs

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # set_cycledefs
    #
    ##########################################
    def set_cycledefs(cycledefs)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Delete all current cycledefs from the database
          dbspecs=db.execute("DELETE FROM cycledef;")
 
          # Add new cycledefs to the database
          cycledefs.each do |cycledef|
            if cycledef[:position].nil?
              db.execute("INSERT INTO cycledef VALUES (NULL,'#{cycledef[:group]}','#{cycledef[:cycledef]}',NULL);")
            else
              db.execute("INSERT INTO cycledef VALUES (NULL,'#{cycledef[:group]}','#{cycledef[:cycledef]}',#{cycledef[:position].to_i});")
            end
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Retrieve all cycles from the cycle table
          dbcycles=db.execute("SELECT cycle,activated,expired,done FROM cycles WHERE cycle == #{reftime.getgm.to_i};")

        end  # database transaction
          
        # Return an array of cycles
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm,{:activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm}) }

        return dbcycles

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Get the starting and ending cycle times
        startcycle=reftime[:start].nil? ? startcycle=Time.gm(1900,1,1,0,0) : reftime[:start].getgm
        endcycle=reftime[:end].nil? ? endcycle=Time.gm(9999,12,31,23,59) : reftime[:end].getgm+1 

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Retrieve all cycles from the cycle table
          dbcycles=db.execute("SELECT cycle,activated,expired,done FROM cycles WHERE cycle >= #{startcycle.getgm.to_i} and cycle <= #{endcycle.getgm.to_i};")

        end  # database transaction
          
        # Return an array of cycles
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm, { :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm }) }

        return dbcycles

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # get_last_cycle
    #
    ##########################################
    def get_last_cycle

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        dbcycles=[]

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Get the maximum cycle time from the database
          max_cycle=db.execute("SELECT MAX(cycle) FROM cycles")[0][0]
          unless max_cycle.nil?
            dbcycles=db.execute("SELECT cycle,activated,expired,done FROM cycles WHERE cycle=#{max_cycle}")
          end

        end  # database transaction
          
        # Return the last cycle
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm, { :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm }) }

        return dbcycles.first

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end

    ##########################################
    #
    # get_active_cycles
    #
    ##########################################
    def get_active_cycles

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        dbcycles=[]

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Get the cycles that are neither done nor expired
          dbcycles=db.execute("SELECT cycle,activated,expired,done FROM cycles WHERE done=0 and expired=0;")

        end  # database transaction
          
        # Return the array of cycle specs
        dbcycles.collect! { |cycle| Cycle.new(Time.at(cycle[0]).getgm, { :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm}) }

        return dbcycles

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end

    ##########################################
    #
    # update_cycles
    #
    ##########################################
    def update_cycles(cycles)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Update each cycle in the database
          cycles.each { |newcycle|
            db.execute("UPDATE cycles SET activated=#{newcycle.activated.to_i},expired=#{newcycle.expired.to_i},done=#{newcycle.done.to_i} WHERE cycle=#{newcycle.cycle.to_i};")
          }

        end  # database transaction
          
      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # add_cycles
    #
    ##########################################
    def add_cycles(cycles)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Add each cycle to the database
          cycles.each { |newcycle|
            db.execute("INSERT INTO cycles VALUES (NULL,#{newcycle.cycle.to_i},#{newcycle.activated.to_i},#{newcycle.expired.to_i},#{newcycle.done.to_i});")
          }

        end  # database transaction
          
      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          if cycles.nil?

            # Retrieve all jobs from the job table
            dbjobs=db.execute("SELECT jobid,taskname,cycle,cores,state,native_state,exit_status,tries,nunknowns FROM jobs;")

          else

            # Retrieve all jobs from the job table that match the cycles provided
            cycles.each do |cycle|
              dbjobs+=db.execute("SELECT jobid,taskname,cycle,cores,state,native_state,exit_status,tries,nunknowns FROM jobs WHERE cycle = #{cycle.to_i};")
            end

          end        

        end  # database transaction

        dbjobs.each do |job|
          task=job[1]
          cycle=Time.at(job[2]).getgm
          jobs[task]={} if jobs[task].nil?
          jobs[task][cycle] = Job.new(job[0],                   # jobid
                                      task,                     # taskname
                                      cycle,                    # cycle
                                      job[3].to_i,              # cores
                                      job[4],                   # state
                                      job[5],                   # native state
                                      job[6].to_i,              # exit_status
                                      job[7].to_i,              # tries
                                      job[8].to_i               # nunknowns
                                     )

        end

        # Return jobs hash
        return jobs

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # add_jobs
    #
    ##########################################
    def add_jobs(jobs)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Add or update each job in the database
          jobs.each do |job|
            db.execute("INSERT INTO jobs VALUES (NULL,'#{job.id}','#{job.task}',#{job.cycle.to_i},#{job.cores},'#{job.state}','#{job.native_state}',#{job.exit_status},#{job.tries},#{job.nunknowns});")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # update_jobs
    #
    ##########################################
    def update_jobs(jobs)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Add or update each job in the database
          jobs.each do |job|
            db.execute("UPDATE jobs SET jobid='#{job.id}',state='#{job.state}',native_state='#{job.native_state}',exit_status=#{job.exit_status},tries=#{job.tries},nunknowns=#{job.nunknowns} WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end

    ##########################################
    #
    # update_jobids
    #
    ##########################################
    def update_jobids(jobs)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Add or update each job in the database
          jobs.each do |job|
            db.execute("UPDATE jobs SET jobid='#{job.jobid}' WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # delete_jobs
    #
    ##########################################
    def delete_jobs(jobs)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Delete each job from the database
          jobs.each do |job|
            db.execute("DELETE FROM jobs WHERE cycle=#{job.cycle.to_i} AND taskname='#{job.task}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Retrieve all bqservers from the job table
          dbbqservers=db.execute("SELECT uri FROM bqservers;")

        end  # database transaction

        # Return jobs hash
        return dbbqservers.flatten

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # add_bqservers
    #
    ##########################################
    def add_bqservers(bqservers)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Add or update each bqserver in the database
          bqservers.each do |bqserver|
            db.execute("INSERT INTO bqservers VALUES (NULL,'#{bqserver}');")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end

    ##########################################
    #
    # delete_bqservers
    #
    ##########################################
    def delete_bqservers(bqservers)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Delete each job from the database
          bqservers.each do |bqserver|
            db.execute("DELETE FROM bqservers WHERE uri='#{bqserver}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Retrieve all downpaths from the job table
          dbdownpaths=db.execute("SELECT path,downdate,host,pid FROM downpaths;")

        end  # database transaction

        # Return an array of downpaths
        dbdownpaths.collect! { |downpath| {:path=>downpath[0], :downtime=>Time.at(downpath[1]).getgm, :host=>downpath[2], :pid=>downpath[3]} }

        # Return downpaths hash
        return dbdownpaths

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # add_downpaths
    #
    ##########################################
    def add_downpaths(downpaths)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Add or update each job in the database
          downpaths.each do |downpath|
            db.execute("INSERT INTO downpaths VALUES (NULL,'#{downpath[:path]}',#{downpath[:downtime].to_i},'#{downpath[:host]}',#{downpath[:pid]});")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end  # begin

    end


    ##########################################
    #
    # delete_downpaths
    #
    ##########################################
    def delete_downpaths(downpaths)

      begin

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Delete each downpath from the database
          downpaths.each do |downpath|
            db.execute("DELETE FROM downpaths WHERE path='#{downpath[:path]}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
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

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Get a listing of the database tables
          dbtables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
          dbtables.flatten!

          # Create an array of rows for each table
          dbtables.each do |table|

            # Initialize the array of rows to be empty
            tables[table.to_sym]=[]

            # Get all rows for the table, where first row is column names
            dbtable=db.execute2("SELECT * FROM #{table};")

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

        end  # database transaction

        # Return the tables
        tables

      rescue SQLite3::BusyException => e
        raise WorkflowMgr::WorkflowDBLockedException,"Could not open workflow database file '#{@database_file}' because it is locked by SQLite"
      end # begin

    end


  private

    ##########################################
    #
    # create_tables
    #
    ##########################################
    def create_tables(db,tables)

      raise "WorkflowSQLite3DB::create_tables must be called inside a transaction" unless db.transaction_active?

      # Create the lock table
      unless tables.member?("lock")
        db.execute("CREATE TABLE lock (pid INTEGER, host VARCHAR(64), time DATETIME);")
      end

      # Create the cycledef table
      unless tables.member?("cycledef")
        db.execute("CREATE TABLE cycledef (id INTEGER PRIMARY KEY, groupname VARCHAR(64), cycledef VARCHAR(256), dirty BOOLEAN);")
      end

      # Create the cycles table
      unless tables.member?("cycles")
        db.execute("CREATE TABLE cycles (id INTEGER PRIMARY KEY, cycle DATETIME, activated DATETIME, expired DATETIME, done DATETIME);")
     end

     # Create the jobs table
     unless tables.member?("jobs")
       db.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, jobid VARCHAR(64), taskname VARCHAR(64), cycle DATETIME, cores INTEGER, state VARCHAR[64], native_state VARCHAR[64], exit_status INTEGER, tries INTEGER, nunknowns INTEGER);")
     end

     # Create the bqservers table
      unless tables.member?("bqservers")
        db.execute("CREATE TABLE bqservers (id INTEGER PRIMARY KEY, uri VARCHAR(1024));")
     end

     # Create the downpaths table
      unless tables.member?("downpaths")
        db.execute("CREATE TABLE downpaths (id INTEGER PRIMARY KEY, path VARCHAR(1024), downdate DATETIME, host VARCHAR[64], pid INTEGER);")
     end

    end  # create_tables

  end  # Class WorkflowSQLite3DB

end  # Module WorkflowMgr
