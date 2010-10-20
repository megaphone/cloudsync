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
    
      def files_to_sync(upload_prefix="")
        $LOGGER.info("Getting files to sync [#{self}]")
        
        files = []
        containers_to_sync(upload_prefix) do |container|
          container = get_or_create_container(container)
          objects_from_container(container, remove_container_name(upload_prefix)) do |path, hash|
            next if hash[:content_type] == "application/directory"

            file = Cloudsync::File.from_cf_info(container, path, hash, self.to_s)
            if block_given?
              yield file
            else
              files << file
            end
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
    
      def get_file_from_store(file)
        Cloudsync::File.from_cf_obj( get_obj_from_store(file), self.to_s )
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
        container_name = upload_prefix.split("/").first
        if container_name
          yield container_name
        else
          last_marker = nil
          loop do
            containers = @store.containers(CONTAINER_LIMIT, last_marker)
            break if containers.empty?
            containers.each do |container| 
              last_marker = container
              yield container
            end
          end
        end
      end
      
      # cf = Cloudsync::Backend::CloudFiles.new( YAML::load_file("cloudsync.yml")[:cloudfiles]); $LOGGER = Logger.new(STDOUT); count = 0; cf.files_to_sync {|p,h| count += 1 }; puts count
      # cf = Cloudsync::Backend::CloudFiles.new( YAML::load_file("cloudsync.yml")[:cloudfiles]); $LOGGER = Logger.new(STDOUT); count = 0; paths = []; cf.files_to_sync("mpsounds-adobe.max.trivia") {|f| count += 1; paths << f.path}; puts count
      
      
      # prefix_path must not include the container name at the beginning of the string
      def objects_from_container(container, prefix_path="", &block)
        $LOGGER.debug("Getting files from #{container.name} (prefix: #{prefix_path})")

        last_marker     = nil
        loop do
          params          = {:limit => OBJECT_LIMIT, :marker => last_marker}
          params[:path]   = prefix_path if !prefix_path.empty?
          
          $LOGGER.debug("OFC #{container.name} (#{prefix_path}) loop: #{params.inspect}")
          
          objects_details = container.objects_detail(params)

          $LOGGER.debug("OFC #{container.name} (#{prefix_path}) got #{objects_details.size}. #{objects_details.class}")
          $LOGGER.debug("-"*50)
          
          break if objects_details.empty?
          
          objects_details.sort.each do |path, hash|
            if hash[:content_type] == "application/directory" && !prefix_path.empty?
              $LOGGER.debug("OFC #{container.name} (#{prefix_path}) recursing into #{path}")
              objects_from_container(container, path, &block)
              $LOGGER.debug("OFC #{container.name} (#{prefix_path}) done recursing into #{path}")
            end
            
            last_marker = path
            yield path, hash
          end
        end
      end

      def get_obj_from_store(file)
        @store.container(file.bucket).object(file.upload_path)
      rescue NoSuchContainerException, NoSuchObjectException => e
        nil
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
