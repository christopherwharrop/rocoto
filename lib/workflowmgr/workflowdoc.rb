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
    require 'workflowmgr/utilities'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(workflowdoc)

      # Get the text from the xml file and put it into a string
      xmlstring=IO.readlines(workflowdoc,nil)[0]

      # Parse the workflow xml string, set option to replace entities
      workflowdoc=LibXML::XML::Document.string(xmlstring,:options => LibXML::XML::Parser::Options::NOENT)

      # Validate the workflow xml document before metatask expansion
      validate_with_metatasks(workflowdoc)

      # Expand metatasks
      expand_metatasks

      # Validate the workflow xml document after metatask expansion
      # The second validation is needed in case metatask expansion introduced invalid XML
      validate_without_metatasks(workflowdoc)

      # Convert the XML tree into a hash
      @workflow=to_h(workflowdoc)

    end  # initialize


    ##########################################
    #
    # method_missing
    # 
    ##########################################
    def method_missing(name,*args)

      dockey=name.to_sym
      if @workflow.has_key?(dockey)
        return @workflow[dockey]
      else
	super
      end

    end


  private


    ##########################################
    #
    # to_h
    # 
    ##########################################
    def to_h(doc)

      # Initialize the workflow hash to contain the <workflow> attributes
      workflow=get_node_attributes(doc.root)

      # Build hashes for the <workflow> child elements
      doc.root.each_element do |child|
        key=child.name.to_sym
	case key
          when :log
            value=log_to_h(child)
          when :cycledef
            value=cycledef_to_h(child)
          when :task
            value=task_to_h(child)
        end
        if workflow.has_key?(key)
          workflow[key]=([workflow[key]] + [value]).flatten
        else
	  workflow[key]=value
        end

      end

      return workflow

    end


    ##########################################
    #
    # get_node_attributes
    # 
    ##########################################
    def get_node_attributes(node)

      # Initialize empty hash
      nodehash={}

      # Loop over node's attributes and set hash key/value pairs
      node.each_attr { |attr| nodehash[attr.name.to_sym]=attr.value }

      return nodehash

    end


    ##########################################
    #
    # log_to_h
    # 
    ##########################################
    def log_to_h(node)

      # Get the log attributes
      log=get_node_attributes(node)
      
      # Get the log path
      log[:path]=compound_time_string_to_h(node)

      return log

    end


    ##########################################
    #
    # cycledef_to_h
    # 
    ##########################################
    def cycledef_to_h(node)

      # Get the cycle attributes
      cycledef=get_node_attributes(node)
      
      # Get the cycle field string
      cycledef[:cycledef]=node.content.strip

      return cycledef

    end


    ##########################################
    #
    # task_to_h
    # 
    ##########################################
    def task_to_h(node)

      # Get the task attributes
      task=get_node_attributes(node)
      
      # Get the task elements
      node.each_element do |child|
        key=child.name.to_sym
        case key
          when :envar
            value=envar_to_h(child)
          when :dependency
            value=dependency_to_h(child).first
          when :cores                              # List integer-only attributes here
	    value=child.content.to_i
          when :id                                 # List string attributes that can't be compound time strings	here
            value=child.content.strip              
          else                                     # Everything else is a compound time string
            value=compound_time_string_to_h(child)
        end

        if task.has_key?(key)
          task[key]=([task[key]] + [value]).flatten
        else
          task[key]=value
        end

      end

      return task

    end


    ##########################################
    #
    # envar_to_h
    # 
    ##########################################
    def envar_to_h(node)

      # Get the envar attributes
      envar=get_node_attributes(node)

      # Get the envar elements
      node.each_element do |child|
        envar[child.name.to_sym]=compound_time_string_to_h(child)
      end

      return envar

    end


    ##########################################
    #
    # dependency_to_h
    # 
    ##########################################
    def dependency_to_h(node)

      dependency=[]
      node.each_element do |child|
        key=child.name.to_sym
        case key
          when :datadep
            value=datadep_to_h(child)
          when :timedep
            value=timedep_to_h(child)
          when :taskdep
            value=taskdep_to_h(child)
          else
            value=get_node_attributes(child)
            value[key]=dependency_to_h(child)
        end
        dependency << value
      end

      return dependency

    end


    #####################################################
    #
    # datadep_to_h
    #
    #####################################################
    def datadep_to_h(node)

      # Get the datadeo attributes
      datadep=get_node_attributes(node)

      datadep[node.name.to_sym]=compound_time_string_to_h(node)

      return datadep

    end


    #####################################################
    #
    # taskdep_to_h
    #
    #####################################################
    def taskdep_to_h(node)

      taskdep=get_node_attributes(node)
      taskdep[node.name.to_sym]=taskdep[:task]
      taskdep.delete(:task)

      return taskdep

    end


    #####################################################
    #
    # timedep_to_h
    #
    #####################################################
    def timedep_to_h(node)

      timedep=get_node_attributes(node)

      # Get the time cycle string
      timedep[node.name.to_sym]=compound_time_string_to_h(node)

      return timedep

    end


    ##########################################
    #
    # compound_time_string_to_h
    # 
    ##########################################
    def compound_time_string_to_h(node)

      # Build an array of strings/hashes
      compound_time_string=node.collect do |child|       
        next if child.content.strip.empty?
        if child.name.to_sym==:text
          child.content.strip
        else          
          { child.name.to_sym=>child.content.strip.gsub(/@(\^?[^@\s])/,'%\1').gsub(/@@/,'@') }.merge(get_node_attributes(child))
        end
      end
      
    end


    ##########################################
    #
    # validate_with_metatasks
    # 
    ##########################################
    def validate_with_metatasks(doc)

      # Parse the Relax NG schema XML document
      relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_with_metatasks.rng")

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      doc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # validate_without_metatasks
    #
    ##########################################
    def validate_without_metatasks(doc)

      # Parse the Relax NG schema XML document
      relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_without_metatasks.rng")

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      doc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # expand_metatasks
    #
    ##########################################
    def expand_metatasks

    end


  end  # Class WorkflowXMLDoc

end  # Module WorkflowMgr
