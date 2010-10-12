module Cloudsync
  class SyncManager
    attr_accessor :from_backend, :to_backend, :dry_run
    
    def initialize(opts={})
      @from_backend     = get_backend opts[:from]
      @to_backend       = get_backend opts[:to]
      @dry_run          = opts[:dry_run]

      $LOGGER           Logger.new(opts[:log_file] || "cloudsync.log")
    end
  
    def sync!
      sync(:sync)
    end
    
    def sync_all!
      sync(:sync_all)
    end
    
    def mirror!
      $LOGGER.info("Mirror started at #{mirror_start = Time.now}. Dry-run? #{!!dry_run?}")
      sync!
      prune!
      $LOGGER.info("Mirror finished at #{Time.now}. Took #{Time.now - mirror_start}s")
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
      
      $LOGGER.info("Prune started at #{prune_start = Time.now}. Dry-run? #{!!dry_run?}")
      $LOGGER.info("Pruning files on #{to_backend} to match files on #{from_backend}")
      
      from_backend_files   = [] # from_backend.files_to_sync(to_backend.upload_prefix)
      to_backend_files     = to_backend.files_to_sync(from_backend.upload_prefix)
      total_files          = to_backend_files.size
      last_decile_complete = 0
      
      to_backend_files.each_with_index do |file, index|
        $LOGGER.debug("Checking if file #{file} exists on [#{from_backend}]")
        if found_file = from_backend.find_file_from_list_or_store(file, from_backend_files)
          $LOGGER.debug("Keeping file #{file} because it was found on #{from_backend}.")
          file_stats[:skipped] << file
        else
          $LOGGER.debug("Removing #{file} because it doesn't exist on #{from_backend}.")
          file_stats[:removed] << file
          
          to_backend.delete(file)
        end
        
        if decile_complete(index, total_files) != last_decile_complete
          last_decile_complete = decile_complete(index, total_files)
          $LOGGER.info("Prune: Completed #{index} files. #{last_decile_complete * 10}% complete")
        end
      end
      
      $LOGGER.info(["Prune finished at #{Time.now}, took #{Time.now - prune_start}s.",
                   "Skipped #{file_stats[:skipped].size} files.",
                   "Removed #{file_stats[:removed].size} files"].join(" "))
      file_stats
    end
    
    def sync(mode)
      file_stats = {:copied => [], :skipped => []}
      $LOGGER.info("Sync started at #{sync_start = Time.now}. Mode: #{mode}. Dry-run? #{!!dry_run?}")
      $LOGGER.info("Syncing from #{from_backend} to #{to_backend}")

      from_backend_files   = from_backend.files_to_sync(to_backend.upload_prefix)
      to_backend_files     = to_backend.files_to_sync(from_backend.upload_prefix)
      total_files          = from_backend_files.size
      last_decile_complete = 0
      
      from_backend_files.each_with_index do |file, index|
        if (mode == :sync_all || to_backend.needs_update?(file, to_backend_files))
          file_stats[:copied] << file
          from_backend.copy(file, to_backend)
        else
          file_stats[:skipped] << file
          $LOGGER.debug("Skipping up-to-date file #{file}")
        end
        
        if decile_complete(index, total_files) != last_decile_complete
          last_decile_complete = decile_complete(index, total_files)
          $LOGGER.info("Sync: Completed #{index} files. #{last_decile_complete * 10}% complete")
        end
      end
      
      $LOGGER.debug(["Sync finished at #{Time.now}, took #{Time.now - sync_start}s.",
                     "Copied #{file_stats[:copied].size} files.",
                     "Skipped #{file_stats[:skipped].size} files."].join(" "))
      file_stats
    end
    
    def decile_complete(index, total_files)
      (index * 100 / total_files) / 10
    end
  end
end
