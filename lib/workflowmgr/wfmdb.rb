#!/usr/bin/ruby

##########################################
#
# module WorkflowDB
#
##########################################
module WorkflowDB

  ##########################################
  #
  # Class WorkflowSQLite3DB
  #
  ##########################################
  class WorkflowSQLite3DB

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file)

      require 'sqlite3'

      db = SQLite3::Database.new(database_file)

    end

  end  # Class WorkflowSQLite3DB

end  # Module WorkflowDB

__WFMDIR__=File.expand_path(File.dirname(__FILE__))

# Add include paths for WFM and libxml-ruby libraries
$:.unshift(__WFMDIR__)
$:.unshift("#{__WFMDIR__}/libxml-ruby/lib")
$:.unshift("#{__WFMDIR__}/libxml-ruby/ext/libxml")
$:.unshift("#{__WFMDIR__}/sqlite3-ruby/lib")
$:.unshift("#{__WFMDIR__}/sqlite3-ruby/ext")

testdb=WorkflowDB::WorkflowSQLite3DB.new("test.db")
puts testdb.inspect
