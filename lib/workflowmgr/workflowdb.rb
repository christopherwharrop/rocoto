##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowSQLite3DB
  #
  ##########################################
  class WorkflowSQLite3DB

    require 'workflowmgr/forkit'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file)

      require 'sqlite3'

      @database_file=database_file
puts File.stat(database_file).ftype.inspect
      begin

        # Open the database and initialize it if necessary
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Get a listing of the database tables
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

            # Create tables if database is new
            create_tables(db) if tables.empty?

          end  # database transaction

        end  # forkit

      rescue WorkflowMgr::ForkitTimeoutException => e
        puts e.message
        sleep(rand)
        retry
      rescue SQLite3::BusyException => e
        puts e.message
        sleep(rand)
        retry 
      rescue SQLite3::SQLException => e
        puts e.message
        sleep(rand)
        retry if e.message=~/cannot start a transaction within a transaction/

      end  # begin

    end  # initialize

    ##########################################
    #
    # test
    #
    ##########################################
    def test

      begin

        result=WorkflowMgr.forkit(5) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)
          
          a=[]

          database.transaction do |db|

            a=db.execute("SELECT a FROM test WHERE id=1")
            if a.empty?
              db.execute("INSERT into test values (?,1)")              
            else
              db.execute("UPDATE test SET a=#{a[0][0]+1}")
            end

          end  # database transaction

          a

        end  # forkit
        
        puts result.inspect

      rescue WorkflowMgr::ForkitTimeoutException => e
        puts e.message
        sleep(rand)
        retry
      rescue SQLite3::BusyException => e
        puts e.message
        sleep(rand)
        retry 
      rescue SQLite3::SQLException => e
        puts e.message
        sleep(rand)
        retry if e.message=~/cannot start a transaction within a transaction/

      end  # begin
  
    end   # test
  
  private

    ##########################################
    #
    # create_tables
    #
    ##########################################
    def create_tables(db)

      raise "WorkflowSQLite3DB::create_tables must be called inside a transaction" unless db.transaction_active?

      db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, a INTEGER);")

    end  # create_tables


  end  # Class WorkflowSQLite3DB

end  # Module WorkflowMgr
