require 'nokogiri'
require 'open-uri'
require 'digest'
require 'zip'

# Utility class for downloading and extracting zip files
class ImportUtils
  def self.fetch_new_urls
    url = 'http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/'
    page = Nokogiri::HTML(open(url))
    current_links = page.css('a').select { |link| link['href'] =~ /^\d*.zip$/ }
    new_file = []
    current_links.each do |filename|
      flag = $redis.sadd('zip_files', filename['href'])
      new_file << filename['href'] if flag
    end
    new_file
  end

  def self.download_new_files(files)
    puts 'Determining if download is required'
    post_urls = build_new_urls(files)
    puts "#{post_urls.count} files needs to be downloaded" if post_urls.present?
    post_urls.each do |post_url|
      zip_path = Rails.root.join('tmp/').join(post_url[:filename])
      download_file(post_url[:file_source], zip_path)
      post_url[:zip_path] = zip_path
    end if post_urls.present?
    post_urls
  end

  def self.build_new_urls(files)
    url_base = 'http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/'
    post_urls = []
    files.each do |file|
      post_info = {}
      post_info[:filename] = file
      post_info[:file_source] = url_base + file
      post_urls.push(post_info)
    end
    post_urls
  end

  def self.download_file(file_source, dest_path)
    puts "Downloading file at #{file_source} to #{dest_path}"
    File.open(dest_path, 'wb') do |saved_file|
      begin
        open(file_source, 'rb') do |read_file|
          saved_file.write(read_file.read)
        end
      rescue Errno::ETIMEDOUT
        puts 'Download timed out. Waiting a minute and then trying again'
        sleep 60
        open(file_source, 'rb') do |read_file|
          saved_file.write(read_file.read)
        end
      end
    end
  end

  def self.extract_files(zip_path, csv_dest_path)
    puts "Unzipping #{zip_path} to #{csv_dest_path}"

    FileUtils.rm_rf(csv_dest_path) if File.exist?(csv_dest_path)

    Zip::File.open(zip_path) do |zip_file|
      zip_file.each do |file|
        file_path = File.join(csv_dest_path, file.name)
        FileUtils.mkdir_p(File.dirname(file_path))
        file.extract(file_path)
      end
    end
  end
end
