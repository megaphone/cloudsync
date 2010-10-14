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
      
      to_backend_files     = to_backend.files_to_sync(from_backend.upload_prefix)
      total_files          = to_backend_files.size
      last_decile_complete = 0
      
      to_backend_files.each_with_index do |file, index|
        $LOGGER.debug("Checking if file #{file} exists on [#{from_backend}]")
        if found_file = from_backend.find_file_from_list_or_store(file)
          $LOGGER.debug("Keeping file #{file} because it was found on #{from_backend}.")
          file_stats[:skipped] << file
        else
          $LOGGER.debug("Removing #{file} because it doesn't exist on #{from_backend}.")
          file_stats[:removed] << file
          
          to_backend.delete(file)
        end
        
        if decile_complete(index, total_files) != last_decile_complete
          last_decile_complete = decile_complete(index, total_files)
          $LOGGER.info("[SM]: Prune: Completed #{index} files. #{last_decile_complete * 10}% complete")
        end
      end
      
      $LOGGER.info(["[SM]: Prune from #{from_backend} to #{to_backend} finished at #{Time.now}, took #{Time.now - prune_start}s.",
                   "Skipped #{file_stats[:skipped].size} files.",
                   "Removed #{file_stats[:removed].size} files"].join(" "))
      file_stats
    end
    
    def sync(mode)
      file_stats = {:copied => [], :skipped => []}
      $LOGGER.info("[SM]: Sync from #{from_backend} to #{to_backend} started at #{sync_start = Time.now}. Mode: #{mode}. Dry-run? #{!!dry_run?}")

      from_backend_files   = from_backend.files_to_sync(to_backend.upload_prefix)
      total_files          = from_backend_files.size
      last_decile_complete = 0
      
      from_backend_files.each_with_index do |file, index|
        if (mode == :sync_all || to_backend.needs_update?(file))
          file_stats[:copied] << file
          from_backend.copy(file, to_backend)
        else
          file_stats[:skipped] << file
          $LOGGER.debug("Skipping up-to-date file #{file}")
        end
        
        if decile_complete(index, total_files) != last_decile_complete
          last_decile_complete = decile_complete(index, total_files)
          $LOGGER.info("[SM]: Sync from #{from_backend} to #{to_backend}: Completed #{index} files. #{last_decile_complete * 10}% complete")
        end
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
