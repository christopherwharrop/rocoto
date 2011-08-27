##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class WorkflowXMLDoc 
  #
  ##########################################
  class WorkflowXMLDoc

    require 'libxml'
    require 'cycle'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(workflowdoc)

      @xmlfile=workflowdoc

      # Get the text from the xml file and put it into a string
      @xmlstring=WorkflowMgr.forkit(2) do
        IO.readlines(@xmlfile,nil)[0]
      end

      # Parse the workflow xml string, set option to replace entities
      @workflowdoc=LibXML::XML::Document.string(@xmlstring,:options => LibXML::XML::Parser::Options::NOENT)

      # Validate the workflow xml document before metatask expansion
      validate_with_metatasks

      # Expand metatasks
      expand_metatasks

      # Validate the workflow xml document after metatask expansion
      # The second validation is needed in case metatask expansion introduced invalid XML
      validate_without_metatasks

    end  # initialize

    ##########################################
    #
    # realtime?
    #
    ##########################################
    def realtime?

      realtime=@workflowdoc.root.attributes['realtime'].downcase =~ /^t|true$/
      !realtime.nil?

    end

    ##########################################
    #
    # cyclethrottle
    #
    ##########################################
    def cyclethrottle

      cyclethrottle=@workflowdoc.root.attributes['cyclethrottle']
      if cyclethrottle.nil?
        return 0
      else
        return cyclethrottle.to_i
      end

    end


    ##########################################
    #
    # scheduler
    #
    ##########################################
    def scheduler

      scheduler=@workflowdoc.root.attributes['scheduler']
      if scheduler.nil?
        return "auto"
      else
        return scheduler.downcase
      end

    end


    ##########################################
    #
    # cycles
    #
    ##########################################
    def cycles

      cycles=[]
      cyclenodes=@workflowdoc.find('/workflow/cycle')
      cyclenodes.each { |cyclenode|
        cyclefields=cyclenode.content.split
        if cyclefields.size==3
          cycles << CycleInterval.new(cyclenode.attributes['group'],cyclefields)
        elsif cyclefields.size==6
          cycles << CycleCron.new(cyclenode.attributes['group'],cyclefields)
        else
	  raise "ERROR: Unsupported <cycle> type!"
        end
      }
      return cycles

    end


  private


    ##########################################
    #
    # validate_with_metatasks
    # 
    ##########################################
    def validate_with_metatasks

      # This method is not wrapped inside a WorkflowMgr.forkit 
      # because it is reading the schemas from the same directory
      # as this source file.  If the schema validation was going to hang,
      # then this code would not be running anyway

      # Parse the Relax NG schema XML document
      relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_with_metatasks.rng")

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      @workflowdoc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def validate_without_metatasks

      # This method is not wrapped inside a WorkflowMgr.forkit 
      # because it is reading the schemas from the same directory
      # as this source file.  If the schema validation was going to hang,
      # then this code would not be running anyway

      # Parse the Relax NG schema XML document
      relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_without_metatasks.rng")

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      @workflowdoc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def expand_metatasks

    end


  end  # Class WorkflowXMLDoc

end  # Module WorkflowMgr