require "cloudfiles"

module Cloudsync
  module Backend
    class CloudFiles < Base
      def initialize(opts={})
        @store = ::CloudFiles::Connection.new \
                  :username => opts[:username],
                  :api_key  => opts[:password]
        super
      end
    
      def download(file)
        start_time = Time.now
        $LOGGER.info("Downloading file #{file}")
      
        tempfile = file.tempfile

        if !dry_run?
          if obj = get_obj_from_store(file)
            obj.save_to_filename(tempfile.path)
            tempfile.close
          else
            $LOGGER.error("Error downloading file #{file}")
            tempfile.unlink and return nil
          end
        end

        $LOGGER.debug("Finished downloading file #{file} from #{self} (#{Time.now - start_time})")
        tempfile
      end
    
      # Put the contents of the path #local_file_path# into
      # the Cloudsync::File object #file# 
      def put(file, local_file_path)
        start_time = Time.now
        $LOGGER.info("Putting #{file} to #{self} (#{file.full_upload_path}).")
        return if dry_run?
      
        get_or_create_obj_from_store(file).
          load_from_filename(local_file_path)
        $LOGGER.debug("Finished putting #{file} to #{self} (#{Time.now - start_time}s)")
      end
    
      def files_to_sync(upload_prefix={})
        $LOGGER.info("Getting files to sync [#{self}]")

        containers_to_sync(upload_prefix).inject([]) do |files, container|
          container = get_or_create_container(container)
          objects_from_container(container, upload_prefix).each do |path, hash|
            files << Cloudsync::File.from_cf_info(container, path, hash)
          end
          files
        end
      end
      
      def delete(file, delete_container_if_empty=true)
        $LOGGER.info("Deleting file #{file}")
        return if dry_run?
      
        container = @store.container(file.container)
        
        container.delete_object(file.path)
      
        if delete_container_if_empty
          container.refresh
          if container.empty?
            $LOGGER.debug("Deleting empty container '#{container.name}'")
            @store.delete_container(container.name)
          end
        end
      
      rescue NoSuchContainerException, NoSuchObjectException => e
        $LOGGER.error("Failed to delete file #{file}")
      end
    
      private
      
      def get_or_create_container(container_name)
        if @store.container_exists?(container_name)
          container = @store.container(container_name)
        else
          container = @store.create_container(container_name)
        end
      end

      def containers_to_sync(upload_prefix)
        upload_prefix[:bucket] ? [upload_prefix[:bucket]] : @store.containers
      end
      
      def objects_from_container(container, upload_prefix)
        objects = []
        if upload_prefix[:prefix]
          container.objects_detail(:path => upload_prefix[:prefix]).collect do |path, hash|
            if hash[:content_type] == "application/directory"
              objects += objects_from_container(container, :prefix => path) 
            else
              objects << [path, hash]
            end
          end
        else
          objects = container.objects_detail
        end
        objects
      end

      def get_obj_from_store(file)
        @store.container(file.bucket).object(file.upload_path)
      rescue NoSuchContainerException, NoSuchObjectException => e
        nil
      end
    
      def get_file_from_store(file)
        Cloudsync::File.from_cf_obj( get_obj_from_store(file) )
      end
    
      def get_or_create_obj_from_store(file)
        container = get_or_create_container(file.container)
      
        if container.object_exists?(file.upload_path)
          container.object(file.upload_path)
        else
          container.create_object(file.upload_path, true)
        end
      end
    end
  end
end
