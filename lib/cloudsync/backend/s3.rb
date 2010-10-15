require "right_aws"

module Cloudsync
  module Backend
    class S3 < Base
      def initialize(opts={})
        @store = RightAws::S3.new(opts[:username],
                                  opts[:password])
        super
      end
    
      def put(file, local_filepath)
        start_time = Time.now
        $LOGGER.info("Putting #{file} to #{self} (#{file.full_upload_path}).")
        return if dry_run?
      
        # Forces creation of the bucket if necessary
        get_or_create_obj_from_store(file)
      
        local_file = ::File.open(local_filepath)
        @store.interface.put(file.bucket, file.upload_path, local_file)
        local_file.close
      
        $LOGGER.debug("Finished putting #{file} to #{self} (#{Time.now - start_time})")
      end
    
      def download(file)
        start_time = Time.now
        $LOGGER.info("Downloading file #{file} (#{file.path})")
      
        tempfile = file.tempfile

        if !dry_run?
          @store.interface.get(file.bucket, file.download_path) do |chunk|
            tempfile.write chunk
          end
        end
      
        tempfile.close
      
        $LOGGER.debug("Finished downloading file #{file} from #{self} (#{Time.now - start_time})")
      
        tempfile
      rescue RightAws::AwsError => e
        $LOGGER.error("Caught error: #{e} (#{file})")
        if e.message =~ /NoSuchKey/
          tempfile.unlink and return nil
        else
          raise
        end
      end
    
      def delete(file, delete_bucket_if_empty=true)
        $LOGGER.info("Deleting #{file}")
        return if dry_run?
      
        get_obj_from_store(file).delete
      
        if bucket = @store.bucket(file.bucket)
          bucket.key(file.download_path).delete
        
          if delete_bucket_if_empty && bucket.keys.empty?
            $LOGGER.debug("Deleting empty bucket '#{bucket.name}'")
            bucket.delete
          end
        end
      rescue RightAws::AwsError => e
        $LOGGER.error("Caught error: #{e} trying to delete #{file}")
      end
      
      def count_files_to_sync(upload_prefix="")
        $LOGGER.debug("Counting files to sync [#{self}]")
        
        buckets_to_sync(upload_prefix).inject(0) do |sum, bucket|
          sum += objects_from_bucket(bucket, upload_prefix).size
        end
      end
    
      def files_to_sync(upload_prefix="")
        $LOGGER.info("Getting files to sync [#{self}]")
        
        buckets_to_sync(upload_prefix).inject([]) do |files, bucket|
          objects_from_bucket(bucket, upload_prefix).collect do |key|
            file = Cloudsync::File.from_s3_obj(key, self.to_s)
            if block_given?
              yield file
            else
              files << file
            end
          end
          files
        end
      end

      # Convenience to grab a single file
      def get_file_from_store(file)
        Cloudsync::File.from_s3_obj( get_obj_from_store(file), self.to_s )
      end

      private
    
      def buckets_to_sync(upload_prefix="")
        bucket_name = upload_prefix.split("/").first
        if bucket_name
          [@store.bucket(bucket_name, true)]
        else
          @store.buckets
        end
      end
      
      def objects_from_bucket(bucket, upload_prefix="")
        prefix_parts = upload_prefix.split("/")
        prefix_parts.shift
        prefix = prefix_parts.join("/")
        
        if !prefix.empty?
          bucket.keys(:prefix => prefix)
        else
          bucket.keys
        end
      end
    
      def get_or_create_obj_from_store(file)
        @store.bucket(file.bucket, true).key(file.upload_path)
      end
    
      def get_obj_from_store(file)
        $LOGGER.debug("gofs, buck: #{file.bucket}. upload path: #{file.upload_path}")
        if bucket = @store.bucket(file.bucket)
          key = bucket.key(file.upload_path)
          return key if key.exists?
        end
      end
    end
  end
end