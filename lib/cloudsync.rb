$:.unshift(File.dirname(__FILE__))

require "cloudsync/sync_manager"
require "cloudsync/version"
require "cloudsync/file"
require "cloudsync/backend/base"
require "cloudsync/backend/cloudfiles"
require "cloudsync/backend/s3"
require "cloudsync/backend/sftp"

# monkeypatches
require "cloudsync/datetime/datetime"