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

  private


    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def validate_with_metatasks

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

      # Parse the Relax NG schema XML document ("_m" means it validates metatask tags)
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