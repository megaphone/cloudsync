require 'tempfile'

module Cloudsync
  module Backend
    class Base
      attr_accessor :store, :sync_manager, :name, :upload_prefix
      
      def initialize(opts = {})
        @sync_manager     = opts[:sync_manager]
        @name             = opts[:name]
        @backend_type     = opts[:backend] || self.class.to_s.split("::").last
        @download_prefix  = opts[:download_prefix] || ""
        @upload_prefix    = opts[:upload_prefix] || ""
      end
      
      # copy
      def copy(file, to_backend)
        start_copy = Time.now
        $LOGGER.info("Copying file #{file} from #{self} to #{to_backend}")
        tempfile = download(file)
        if tempfile
          to_backend.put(file, tempfile.path)

          $LOGGER.debug("Finished copying #{file} from #{self} to #{to_backend} (#{Time.now - start_copy}s)")
          tempfile.unlink
        else
          $LOGGER.info("Failed to download #{file}")
        end
      end
    
      def to_s
        "#{@name}[:#{@backend_type}/#{@upload_prefix}]"
      end
      
      # needs_update?
      def needs_update?(file)
        $LOGGER.debug("Checking if #{file} needs update")
      
        local_backend_file = get_file_from_store(file)

        if local_backend_file.nil?
          $LOGGER.debug("File doesn't exist at #{self} (#{file})")
          return true
        end

        if file.e_tag == local_backend_file.e_tag
          $LOGGER.debug("Etags match for #{file}")
          return false
        else
          $LOGGER.debug(["Etags don't match for #{file}.",
                        "#{file.backend}: #{file.e_tag}",
                        "#{self}: #{local_backend_file.e_tag}"].join(" "))
          return true
        end
      end
    
      # download
      def download(file)
        raise NotImplementedError
      end
    
      # put
      def put(file, local_filepath)
        raise NotImplementedError
      end
    
      # delete
      def delete(file, delete_bucket_if_empty=true)
        raise NotImplementedError
      end
    
      # all_files
      def all_files
        raise NotImplementedError
      end
      
      def files_to_sync(upload_prefix={})
        all_files
      end
      
      private
      
      def dry_run?
        return false unless @sync_manager
        @sync_manager.dry_run?
      end

      # get_file_from_store
      def get_file_from_store(file)
        raise NotImplementedError
      end
    end
  end
end
