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
  # Class WorkflowSQLite3DB
  #
  ##########################################
  class WorkflowSQLite3DB

    require 'sqlite3'
    require "socket"

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

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            db.execute("INSERT INTO lock VALUES (#{Process.ppid},'#{Socket.gethostname}',#{Time.now.to_i});") 
          else
            raise WorkflowMgr::WorkflowLockedException, "ERROR: Workflow is locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}"
          end

        end  # database transaction

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        exit 1
      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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

          if Process.ppid==lock[0][0] && Socket.gethostname==lock[0][1]
            db.execute("DELETE FROM lock;")      
          else
            raise WorkflowMgr::WorkflowLockedException, "ERROR: Process #{Process.ppid} cannot unlock the workflow because it does not own the lock." +
                                                        "       The workflow is already locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}."
          end

        end  # database transaction

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        exit 1
      rescue SQLite3::BusyException
        STDERR.puts 
        STDERR.puts "ERROR: Could not unlock the workflow.  The database is locked by SQLite."
        STDERR.puts
        exit 1
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
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      end  # begin
  
    end


    ##########################################
    #
    # get_cycles
    #
    ##########################################
    def get_cycles(reftime=Time.gm(1900,1,1,0,0))

      begin

        dbcycles=[]

        # Get a handle to the database
        database = SQLite3::Database.new(@database_file)

        # Start a transaction so that the database will be locked
        database.transaction do |db|

          # Retrieve all cycles from the cycle table
          dbcycles=db.execute("SELECT cycle,activated,expired,done FROM cycles WHERE cycle >= #{reftime.getgm.to_i};")

        end  # database transaction
          
        # Return an array of cycles
        dbcycles.collect! { |cycle| {:cycle=>Time.at(cycle[0]).getgm, :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm} }

        return dbcycles

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
        dbcycles.collect! { |cycle| {:cycle=>Time.at(cycle[0]).getgm, :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm} }

        return dbcycles.first

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      end  # begin

    end

    ##########################################
    #
    # get_active_cycles
    #
    ##########################################
    def get_active_cycles(cycle_lifespan=nil)

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
        dbcycles.collect! { |cycle| {:cycle=>Time.at(cycle[0]).getgm, :activated=>Time.at(cycle[1]).getgm, :expired=>Time.at(cycle[2]).getgm, :done=>Time.at(cycle[3]).getgm} }

        return dbcycles

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            db.execute("UPDATE cycles SET activated=#{newcycle[:activated].to_i},expired=#{newcycle[:expired].to_i},done=#{newcycle[:done].to_i} WHERE cycle=#{newcycle[:cycle].to_i};")
          }

        end  # database transaction
          
      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            db.execute("INSERT INTO cycles VALUES (NULL,#{newcycle.to_i},#{Time.now.to_i},0,0);")
          }

        end  # database transaction
          
      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            dbjobs=db.execute("SELECT jobid,taskname,cycle,cores,state,exit_status,tries FROM jobs;")

          else

            # Retrieve all jobs from the job table that match the cycles provided
            cycles.each do |cycle|
              dbjobs+=db.execute("SELECT jobid,taskname,cycle,cores,state,exit_status,tries FROM jobs WHERE cycle = #{cycle.to_i};")
            end

          end        

        end  # database transaction

        dbjobs.each do |job|
          jobid=job[0]
          jobtask=job[1]
          jobcycle=Time.at(job[2]).getgm
          jobcores=job[3].to_i
          jobstate=job[4]
          jobstatus=job[5].to_i
          jobtries=job[6].to_i
          jobs[jobtask]={} if jobs[jobtask].nil?
          jobs[jobtask][jobcycle]={} if jobs[jobtask][jobcycle].nil?
          jobs[jobtask][jobcycle][:jobid]=jobid
          jobs[jobtask][jobcycle][:taskname]=jobtask
          jobs[jobtask][jobcycle][:cycle]=jobcycle
          jobs[jobtask][jobcycle][:cores]=jobcores
          jobs[jobtask][jobcycle][:state]=jobstate
          jobs[jobtask][jobcycle][:exit_status]=jobstatus
          jobs[jobtask][jobcycle][:tries]=jobtries
        end

        # Return jobs hash
        return jobs

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            db.execute("INSERT INTO jobs VALUES (NULL,'#{job[:jobid]}','#{job[:taskname]}',#{job[:cycle].to_i},#{job[:cores]},'#{job[:state]}',#{job[:exit_status]},#{job[:tries]});")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            db.execute("UPDATE jobs SET jobid='#{job[:jobid]}',state='#{job[:state]}',exit_status=#{job[:exit_status]},tries=#{job[:tries]} WHERE cycle=#{job[:cycle].to_i} AND taskname='#{job[:taskname]}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
            db.execute("DELETE FROM jobs WHERE cycle=#{job[:cycle].to_i} AND taskname='#{job[:taskname]}';")
          end

        end  # database transaction

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
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
        db.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, jobid VARCHAR(64), taskname VARCHAR(64), cycle DATETIME, cores INTEGER, state VARCHAR[64], exit_status INTEGER, tries INTEGER);")
     end

    end  # create_tables

  end  # Class WorkflowSQLite3DB

end  # Module WorkflowMgr
