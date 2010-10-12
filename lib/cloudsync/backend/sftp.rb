require 'net/ssh'
require 'net/sftp'

module Cloudsync::Backend
  class Sftp < Base
    attr_accessor :host, :username, :password
    
    def initialize(options = {})
      @host                 = options[:host]
      @base_path            = options[:base_path]
      @username             = options[:username]
      @password             = options[:password]
      prefix_parts = options[:upload_prefix].split("/")
      
      @bucket = prefix_parts.shift
      @prefix = prefix_parts.join("/")
      
      super
    end
    
    # download
    def download(file)
      $LOGGER.info("Downloading #{file}")
      tempfile = file.tempfile
      
      if !dry_run?
        Net::SSH.start(@host, @username, :password => @password) do |ssh|
          ssh.sftp.connect do |sftp|
            begin
              sftp.download!(absolute_path(file.path), tempfile)
            rescue RuntimeError => e
              if e.message =~ /permission denied/
                tempfile.close
                return tempfile
              else
                raise
              end
            end
          end
        end
      end
      tempfile.close
      tempfile
    end
    
    # put
    def put(file, local_filepath)
      $LOGGER.info("Putting #{file} to #{self}")
      return if dry_run?
      
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          sftp.upload!(local_filepath, absolute_path(file.path))
        end
      end
    end
    
    # delete
    def delete(file, delete_bucket_if_empty=true)
      $LOGGER.info("Deleting #{file}")
      return if dry_run?
      
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          sftp.remove!(absolute_path(file.path))
        end
      end
    end
    
    def files_to_sync(upload_prefix={})
      $LOGGER.info("Getting files to sync [#{self}]")
      files = []
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          filepaths = sftp.dir.glob(@base_path, "**/**").collect {|entry| entry.name}
        
          files = filepaths.collect do |filepath|
            attrs = sftp.stat!(absolute_path(filepath))
            next unless attrs.file?

            e_tag = ssh.exec!("md5sum #{absolute_path(filepath)}").split(" ").first
            Cloudsync::File.new \
              :bucket        => @bucket,
              :path          => filepath,
              :size          => attrs.size,
              :last_modified => attrs.mtime,
              :prefix        => @prefix,
              :e_tag         => e_tag,
              :store         => Cloudsync::Backend::Sftp
          end.compact
        end
      end
      files
    end
    
    def absolute_path(path)
      @base_path + "/" + path
    end
    
    private
    
    # get_file_from_store
    def get_file_from_store(file)
      local_filepath = file.path.sub(/^#{@prefix}\/?/,"")
      
      $LOGGER.debug("Looking for local filepath: #{local_filepath}")
      $LOGGER.debug("Abs filepath: #{absolute_path(local_filepath)}")
      
      sftp_file = nil
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          begin
            attrs = sftp.stat!(absolute_path(local_filepath))
          rescue Net::SFTP::StatusException => e
            break if e.message =~ /no such file/
            raise
          end
          break unless attrs.file?
          
          e_tag = ssh.exec!("md5sum #{absolute_path(local_filepath)}").split(" ").first
          sftp_file = Cloudsync::File.new \
            :bucket        => @bucket,
            :path          => local_filepath,
            :size          => attrs.size,
            :last_modified => attrs.mtime,
            :prefix        => @prefix,
            :e_tag         => e_tag,
            :store         => Cloudsync::Backend::Sftp
        end
      end
      sftp_file
    end
  end 
end
