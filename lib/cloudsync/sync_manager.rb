require 'yaml'

module Cloudsync
  class SyncManager
    attr_accessor :from_backend, :to_backend, :dry_run
    
    def initialize(opts={})
      @from_backend     = get_backend opts[:from]
      @to_backend       = get_backend opts[:to]
      
      if @from_backend == @to_backend
        raise ArgumentError, "The from_backend can't be the same as the to_backend."
      end
      
      @dry_run          = opts[:dry_run]

      log_file = opts[:log_file] || "cloudsync.log"
      log_file = ::File.expand_path(log_file)
      $LOGGER  = Logger.new(log_file)
    end
  
    def sync!
      sync(:sync)
    end
    
    def sync_all!
      sync(:sync_all)
    end
    
    def mirror!
      $LOGGER.info("[SM]: Mirror from #{from_backend} to #{to_backend} started at #{mirror_start = Time.now}. Dry-run? #{!!dry_run?}")
      sync!
      prune!
      $LOGGER.info("[SM]: Mirror from #{from_backend} to #{to_backend} finished at #{Time.now}. Took #{Time.now - mirror_start}s")
    end
    
    def dry_run?
      @dry_run
    end
    
    def prune!
      prune
    end
    
    private
    
    def get_backend(backend_name)
      opts = configs[backend_name].merge(:name => backend_name, :sync_manager => self)
      
      case opts[:backend]
      when :s3
        Cloudsync::Backend::S3.new(opts)
      when :cloudfiles
        Cloudsync::Backend::CloudFiles.new(opts)
      when :sftp
        Cloudsync::Backend::Sftp.new(opts)
      end
    end
    
    def configs
      @configs ||= begin
        if ::File.exists?( path = ::File.expand_path("~/.cloudsync.yml") )
          YAML::load_file(path)
        elsif ::File.exists?( path = ::File.expand_path("cloudsync.yml") )
          YAML::load_file(path)
        else
          raise "Couldn't find cloudsync.yml file!"
        end
      end
    end
    
    def prune
      file_stats = {:removed => [], :skipped => []}
      
      $LOGGER.info("[SM]: Prune from #{from_backend} to #{to_backend} started at #{prune_start = Time.now}. Dry-run? #{!!dry_run?}")
      
      index                = 1

      to_backend.files_to_sync(from_backend.upload_prefix) do |file|
        $LOGGER.debug("Checking if file exists on backend: #{file} [#{from_backend}]")
        if found_file = from_backend.get_file_from_store(file)
          $LOGGER.debug("Keeping file because it was found on backend: #{file} [#{from_backend}].")
          file_stats[:skipped] << file
        else
          $LOGGER.debug("Removing file because it doesn't exist on backend: #{file} [#{from_backend}].")
          file_stats[:removed] << file
          
          to_backend.delete(file)
        end
        
        if index % 1000 == 0
          $LOGGER.info("[SM]: Prune: Completed #{index} files (skipped: #{file_stats[:skipped].size}, removed: #{file_stats[:removed].size}).")
        end
        
        if index % Cloudsync::Backend::Base::OBJECT_LIMIT == 0
          $LOGGER.debug("GC starting")
          GC.start
          
          memory_usage = `ps -o rss= -p #{$$}`.to_i
          ocount = 0; ObjectSpace.each_object {|o| ocount += 1}
          $LOGGER.debug "memory: #{memory_usage}, objects: #{ocount}"
        end
        
        index += 1
      end
      
      $LOGGER.info(["[SM]: Prune from #{from_backend} to #{to_backend} finished at #{Time.now}, took #{Time.now - prune_start}s.",
                   "Skipped #{file_stats[:skipped].size} files.",
                   "Removed #{file_stats[:removed].size} files"].join(" "))
      file_stats
    end
    
    def sync(mode)
      file_stats = {:copied => [], :skipped => []}
      $LOGGER.info("[SM]: Sync from #{from_backend} to #{to_backend} started at #{sync_start = Time.now}. Mode: #{mode}. Dry-run? #{!!dry_run?}")

      index = 1

      from_backend.files_to_sync(to_backend.upload_prefix) do |file|
        if (mode == :sync_all || to_backend.needs_update?(file))
          file_stats[:copied] << file
          from_backend.copy(file, to_backend)
        else
          file_stats[:skipped] << file
          $LOGGER.debug("Skipping up-to-date file #{file}")
        end
        
        if index % 1000 == 0
          $LOGGER.info("[SM]: Sync from #{from_backend} to #{to_backend}: Completed #{index} files (skipped: #{file_stats[:skipped].size}, copied: #{file_stats[:copied].size}).")
        end
        
        index += 1
      end
      
      $LOGGER.info(["[SM]: Sync from #{from_backend} to #{to_backend} finished at #{Time.now}, took #{Time.now - sync_start}s.",
                     "Copied #{file_stats[:copied].size} files.",
                     "Skipped #{file_stats[:skipped].size} files."].join(" "))
      file_stats
    end
    
    def decile_complete(index, total_files)
      (index * 100 / total_files) / 10
    end
  end
end
