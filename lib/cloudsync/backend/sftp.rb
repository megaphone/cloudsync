require 'net/ssh'
require 'net/sftp'
require 'escape'

module Cloudsync::Backend
  class Sftp < Base
    attr_accessor :host, :username, :password
    
    def initialize(options = {})
      @host                 = options[:host]
      @username             = options[:username]
      @password             = options[:password]
      
      super
    end
    
    # download
    def download(file)
      $LOGGER.info("Downloading #{file} from #{file.download_path}")
      tempfile = file.tempfile
      
      if !dry_run?
        Net::SSH.start(@host, @username, :password => @password) do |ssh|
          ssh.sftp.connect do |sftp|
            begin
              sftp.download!(file.download_path, tempfile)
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
      $LOGGER.info("Putting #{file} to #{self} (#{file.upload_path})")
      return if dry_run?
      
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          sftp.upload!(local_filepath, file.upload_path)
        end
      end
    end
    
    # delete
    def delete(file, delete_bucket_if_empty=true)
      $LOGGER.info("Deleting #{file}")
      return if dry_run?
      
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          sftp.remove!(file.download_path)
        end
      end
    end
    
    def files_to_sync(upload_prefix="")
      $LOGGER.info("Getting files to sync [#{self}]")
      files = []
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          filepaths = sftp.dir.glob(@download_prefix, "**/**").collect {|entry| entry.name}
        
          filepaths.each do |filepath|
            attrs = sftp.stat!(local_filepath_from_filepath(filepath))
            next unless attrs.file?

            e_tag = ssh.exec!(md5sum_cmd(filepath)).split(" ").first
            file = Cloudsync::File.new \
              :path            => filepath,
              :upload_prefix   => @upload_prefix,
              :download_prefix => @download_prefix,
              :size            => attrs.size,
              :last_modified   => attrs.mtime,
              :e_tag           => e_tag,
              :backend         => self.to_s,
              :backend_type    => Cloudsync::Backend::Sftp
            
            if block_given?
              yield file
            else
              files << file
            end
          end
        end
      end
      files
    end
    
    # get_file_from_store
    def get_file_from_store(file)
      $LOGGER.debug("Looking for local filepath: #{local_filepath_from_filepath(file.full_download_path)}")

      sftp_file = nil
      Net::SSH.start(@host, @username, :password => @password) do |ssh|
        ssh.sftp.connect do |sftp|
          begin
            attrs = sftp.stat!(local_filepath_from_filepath(file.full_download_path))
          rescue Net::SFTP::StatusException => e
            break if e.message =~ /no such file/
            raise
          end
          break unless attrs.file?
          
          sftp_file = Cloudsync::File.new \
            :path            => file.download_path,
            :upload_prefix   => @upload_prefix,
            :download_prefix => @download_prefix,
            :size            => attrs.size,
            :last_modified   => attrs.mtime,
            :e_tag           => ssh.exec!(md5sum_cmd(file.download_path)).split(" ").first,
            :backend         => self.to_s,
            :backend_type    => Cloudsync::Backend::Sftp
        end
      end
      sftp_file
    end
    
    private
    
    def md5sum_cmd(filepath)
      Escape.shell_command(["md5sum","#{local_filepath_from_filepath(filepath)}"])
    end
    
    def local_filepath_from_filepath(filepath)
      stripped_path = filepath.sub(/^#{@upload_prefix}\/?/,"")
      if @download_prefix
        "#{@download_prefix}/#{stripped_path}"
      else
        stripped_path
      end
    end
  end 
end
