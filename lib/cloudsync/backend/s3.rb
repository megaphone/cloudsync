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
        $LOGGER.info("Downloading file #{file}")
      
        tempfile = file.tempfile

        if !dry_run?
          @store.interface.get(file.bucket, file.path) do |chunk|
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
          bucket.key(file.path).delete
        
          if delete_bucket_if_empty && bucket.keys.empty?
            $LOGGER.debug("Deleting empty bucket '#{bucket.name}'")
            bucket.delete
          end
        end
      rescue RightAws::AwsError => e
        $LOGGER.error("Caught error: #{e} trying to delete #{file}")
      end
    
      def files_to_sync(upload_prefix={})
        $LOGGER.info("Getting files to sync [#{self}]")
      
        buckets_to_sync(upload_prefix).inject([]) do |files, bucket|
          objects_from_bucket(bucket, upload_prefix).collect do |key|
            files << Cloudsync::File.from_s3_obj(key, self.to_s)
          end
          files
        end
      end

      private
    
      def buckets_to_sync(upload_prefix)
        if upload_prefix[:bucket]
          [@store.bucket(upload_prefix[:bucket], true)]
        else
          @store.buckets
        end
      end
      
      def objects_from_bucket(bucket, upload_prefix)
        if upload_prefix[:prefix]
          bucket.keys(:prefix => upload_prefix[:prefix])
        else 
          bucket.keys
        end
      end
    
      # Convenience to grab a single file
      def get_file_from_store(file)
        Cloudsync::File.from_s3_obj( get_obj_from_store(file), self.to_s )
      end
    
      def get_or_create_obj_from_store(file)
        @store.bucket(file.bucket, true).key(file.upload_path)
      end
    
      def get_obj_from_store(file)
        if bucket = @store.bucket(file.bucket)
          key = bucket.key(file.upload_path)
          return key if key.exists?
        end
      end
    end
  end
end