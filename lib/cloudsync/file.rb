module Cloudsync
  class File
    attr_accessor :bucket, :path, :size, :last_modified, :e_tag, :backend
    alias_method :container, :bucket
    alias_method :container=, :bucket=
    
    def initialize(options={})
      @bucket        = options[:bucket]
      @path          = options[:path] 
      @size          = options[:size]
      @last_modified = options[:last_modified]
      @e_tag         = options[:e_tag]
      @backend       = options[:backend]
    end
    
    def self.from_s3_obj(obj, backend=nil)
      return nil if obj.nil?
      new({
        :bucket        => obj.bucket.name,
        :path          => obj.name,
        :size          => obj.size,
        :last_modified => obj.last_modified.to_i,
        :e_tag         => obj.e_tag.gsub('"',''),
        :backend       => backend})
    end
    
    def self.from_cf_info(container, path, hash, backend)
      new({ :bucket        => container.name,
            :path          => path,
            :size          => hash[:bytes],
            :last_modified => hash[:last_modified].to_gm_time.to_i,
            :e_tag         => hash[:hash],
            :backend       => backend })
    end
    
    def self.from_cf_obj(obj, backend=nil)
      return nil if obj.nil?
      new({
        :bucket        => obj.container.name,
        :path          => obj.name,
        :size          => obj.bytes.to_i,
        :last_modified => obj.last_modified.to_i,
        :e_tag         => obj.etag,
        :backend       => backend})
    end
    
    def to_s
      "#{path}"
    end
    
    def unique_filename
      [bucket,e_tag,path].join.gsub(/[^a-zA-Z\-_0-9]/,'')
    end
    
    def full_name
      [bucket,path].join("/")
    end
    
    def upload_path
      if @prefix
        @prefix + "/" + @path
      else
        @path
      end
    end
    
    def full_upload_path
      [bucket, upload_path].join("/")
    end
    
    def tempfile
      Tempfile.new(unique_filename)
    end
  end
end