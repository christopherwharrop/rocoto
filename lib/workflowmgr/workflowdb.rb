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
    require 'workflowmgr/forkit'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file)

      @database_file=database_file

      begin

        # Fork a process to access the database and initialize it
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Get a listing of the database tables
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

            # Create all tables that are missing from the database 
            create_tables(db,tables.flatten)

          end  # database transaction

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        exit -1
      end  # begin

    end  # initialize


    ##########################################
    #
    # lock_workflow
    #
    ##########################################
    def lock_workflow

      begin

        # Fork a process to access the database and acquire the workflow lock
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Access the workflow lock maintained by the workflow manager
            lock=db.execute("SELECT * FROM lock;")      

            # If no lock is present, we have acquired the lock.  Write the WFM's pid and host into the lock table
            if lock.empty?
              db.execute("INSERT INTO lock VALUES (#{Process.ppid},'#{Socket.gethostname}','#{Time.now.to_s}');") 
            else
	      raise WorkflowMgr::WorkflowLockedException, "ERROR: Workflow is locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{lock[0][2]}"
            end

          end  # database transaction

        end  # forkit

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        exit 1
      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        exit -1
      end  # begin

    end


    ##########################################
    #
    # unlock_workflow
    #
    ##########################################
    def unlock_workflow

      begin

        # Open the database and initialize it if necessary
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            lock=db.execute("SELECT * FROM lock;")      

            if Process.ppid==lock[0][0] && Socket.gethostname==lock[0][1]
              db.execute("DELETE FROM lock;")      
            else
              raise WorkflowMgr::WorkflowLockedException, "ERROR: Process #{Process.ppid} cannot unlock the workflow because it does not own the lock." +
                                                          "       The workflow is already locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{lock[0][2]}."
            end

          end  # database transaction

        end  # forkit

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        exit 1
      rescue SQLite3::BusyException
        STDERR.puts 
        STDERR.puts "ERROR: Could not unlock the workflow.  The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        exit -1

      end  # begin

    end

    ##########################################
    #
    # update_cyclespecs
    #
    ##########################################
    def update_cyclespecs(newspecs)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Retreive the current cycle specs from the database
            dbspecs=db.execute("SELECT groupname,fieldstr FROM cyclespec;")
 
            # Delete cycles specs from the database that are not in the new cycle spec list
            dbspecs.each do |spec|
              unless newspecs.member?({:group=>spec[0],:fieldstr=>spec[1]})
                db.execute("DELETE FROM cyclespec WHERE groupname='#{spec[0]}' and fieldstr='#{spec[1]}';")
              end
            end

            # Add incoming cycles that are not in database and initialize them as dirty
            newspecs.each do |spec|
              unless dbspecs.member?([spec[:group],spec[:fieldstr]])
                db.execute("INSERT INTO cyclespec VALUES (NULL,'#{spec[:group]}','#{spec[:fieldstr]}',1);")
              end
            end

          end  # database transaction

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin
  
    end


    ##########################################
    #
    # get_cyclespecs
    #
    ##########################################
    def get_cyclespecs

      begin

        cyclespecs=[]

        # Fork a process to access the database to retrieve the cyclespecs
        cyclespecs=WorkflowMgr.forkit(2) do

          dbspecs=[]

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Access the workflow lock maintained by the workflow manager
            dbspecs=db.execute("SELECT groupname,fieldstr,dirty FROM cyclespec;")

          end  # database transaction
          
          # Return the array of cycle specs
          dbspecs.collect { |spec| {:group=>spec[0], :fieldstr=>spec[1], :dirty=>spec[2]} }

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

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

      # Create the cyclespec table
      unless tables.member?("cyclespec")
        db.execute("CREATE TABLE cyclespec (id INTEGER PRIMARY KEY, groupname VARCHAR(64), fieldstr VARCHAR(256), dirty BOOLEAN);")
      end

    end  # create_tables

  end  # Class WorkflowSQLite3DB

end  # Module WorkflowMgr
