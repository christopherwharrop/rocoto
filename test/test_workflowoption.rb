if RUBY_VERSION < "1.9.0"
  require 'require_relative'
end

require 'test/unit'
require_relative '../lib/workflowmgr/workflowoption'

class TestWorkflowOptions < Test::Unit::TestCase

  def test_workflow_opt

    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","-v"])
    assert_equal("workflowpath",options.workflowdoc)
    options=WorkflowMgr::WorkflowOption.new(["--workflow","workflowpath","-d","databasepath","-v"])
    assert_equal("workflowpath",options.workflowdoc)
    options=WorkflowMgr::WorkflowOption.new(["--workflow=workflowpath","-d","databasepath","-v"])
    assert_equal("workflowpath",options.workflowdoc)

  end


  def test_database_opt

    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","-v"])
    assert_equal("databasepath",options.database)
    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","--database","databasepath","-v"])
    assert_equal("databasepath",options.database)
    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","--database=databasepath","-v"])
    assert_equal("databasepath",options.database)

  end


  def test_verbose_opt

    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","-v"])
    assert_equal(1,options.verbose)
    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","-v","10"])
    assert_equal(10,options.verbose)
    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","--verbose"])
    assert_equal(1,options.verbose)
    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","--verbose","10"])
    assert_equal(10,options.verbose)
    options=WorkflowMgr::WorkflowOption.new(["-w","workflowpath","-d","databasepath","--verbose=10"])
    assert_equal(10,options.verbose)

  end

end