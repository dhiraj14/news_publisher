require 'import_utils'

namespace :zip_files_import do
  desc 'imports all the current posts zip files'
  task post_content: :environment do
    ActiveRecord::Base.logger.level = 2

    start = Time.now

    new_urls = ImportUtils.fetch_new_urls
    recent_files = ImportUtils.download_new_files(new_urls) if new_urls.present?
    dest_paths = []
    if recent_files.present?
      recent_files.each do |files|
        puts "Beginning extraction from #{files[:filename]}"
        dest_path = Rails.root.join('tmp/posts/').join(files[:filename].chomp('.zip'))
        ImportUtils.extract_files(files[:zip_path], dest_path)
        dest_paths << dest_path
      end
      puts 'Done with File extraction'
    end
    publish_to_list(dest_paths) if dest_paths.present?

    stop = Time.now
    puts "Task executed in #{(stop - start)} seconds"
  end
end

def publish_to_list(dest_paths)
  file_count = 0
  puts "Start Publishing Xml Content to List: \n"
  dest_paths.each do |path|
    Dir.glob("#{path}/*.xml") do |xml_file|
      file_count += 1
      puts "publishing content for #{xml_file.split('/').last}"
      file_content = File.read(xml_file)
      $redis.lpush('NEWS_XML', file_content)
    end
  end
  puts "\n Published #{file_count} files to NEWS_XML list"
end
