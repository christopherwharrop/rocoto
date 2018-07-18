##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class BatchSystem
  #
  # Simple base class for all batch systems
  # ensures a self.feature? function exists.
  # Default returns false for all 
  #
  ##########################################
  class BatchSystem
    def self.feature?(flag)
      return false
    end

    def boot_warning
      return nil # by default, allow rocotoboot
    end

    def reap()
    end
  end
end
