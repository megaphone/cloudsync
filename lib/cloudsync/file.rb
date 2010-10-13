module Cloudsync
  class File
    attr_accessor :path, :size, :last_modified, :e_tag, :backend
    
    def initialize(options = {})
      @path                = options[:path] 
      @size                = options[:size]
      @last_modified       = options[:last_modified]
      @e_tag               = options[:e_tag]
      @backend             = options[:backend]
      @upload_prefix       = options[:upload_prefix]
      @download_prefix     = options[:download_prefix]
      @backend_type        = options[:backend_type]
    end
    
    def self.from_s3_obj(obj, backend=nil)
      return nil if obj.nil?
      new({
        :upload_prefix => obj.bucket.name,
        :path          => obj.name,
        :size          => obj.size,
        :last_modified => obj.last_modified.to_i,
        :e_tag         => obj.e_tag.gsub('"',''),
        :backend       => backend,
        :backend_type  => Cloudsync::Backend::S3})
    end
    
    def self.from_cf_info(container, path, hash, backend)
      new({ 
            :upload_prefix => container.name,
            :path          => path,
            :size          => hash[:bytes],
            :last_modified => hash[:last_modified].to_gm_time.to_i,
            :e_tag         => hash[:hash],
            :backend       => backend,
            :backend_type  => Cloudsync::Backend::CloudFiles })
    end
    
    def self.from_cf_obj(obj, backend=nil)
      return nil if obj.nil?
      new({
        :upload_prefix => obj.container.name,
        :path          => obj.name,
        :size          => obj.bytes.to_i,
        :last_modified => obj.last_modified.to_i,
        :e_tag         => obj.etag,
        :backend       => backend,
        :backend_type  => Cloudsync::Backend::CloudFiles})
    end
    
    def to_s
      "#{full_upload_path}"
    end
    
    def unique_filename
      [bucket,e_tag,path].join.gsub(/[^a-zA-Z\-_0-9]/,'')
    end
    
    def full_name
      [bucket,path].join("/")
    end
    
    def bucket
      @bucket ||= begin
        @upload_prefix.split("/").first
      end
    end
    alias_method :container, :bucket
    
    def upload_path
      without_bucket_path = @upload_prefix.sub(/^#{bucket}\/?/,"")
      if without_bucket_path.empty?
        @path
      else
        without_bucket_path + "/" + @path
      end
    end
    
    def download_path
      @download_prefix ? "#{@download_prefix}/#{@path}" : @path
    end
    
    def upload_path_without_bucket
      
    end
    
    def full_download_path
      [bucket, download_path].join("/")
    end
    
    def full_upload_path
      [bucket, upload_path].join("/")
    end
    
    def tempfile
      Tempfile.new(unique_filename)
    end
  end
end
