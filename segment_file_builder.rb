# frozen_string_literal: true

require 'csv'
require 'json'
require 'faraday'
require 'dotenv/load'

###
class SegmentFileBuilder
  attr_reader :technical_configuration, :mapping_table

  def initialize(configuration_file)
    @technical_configuration = load_config(configuration_file)
    load_context
    @mapping_table = {
      JUILLET1: '18158309',
      JUILLET2: '18158310',
      JUILLET3: '18158311',
      JUILLET4: '18158312',
      JUILLET5: '18158313',
      JUILLET7: '18158314'
    }
  end

  def create_segment_file(input_file)
    segment_file = "#{input_file.chomp('.csv')}.ndjson"
    File.new(segment_file, 'w+')
    identifiers = parse_file("#{technical_configuration[:input_files_foler]}/#{input_file}")
    identifiers.each do |id|
      add_id_to_segment_file(id, segment_file) if userpoint_exists?(id)
    end
  end

  def perform
    input_files = Dir.children(technical_configuration[:input_files_foler])
    input_files.each do |file|
      create_segment_file(file)
    end
  end

  private

  def load_config(configuration_file)
    configuration = {}
    pre_configuration = configuration_file ? Dotenv.parse(configuration_file) : Dotenv.parse('.env')
    pre_configuration.each_key do |key|
      configuration[key.to_sym] = pre_configuration[key]
    end
    configuration
  end

  def load_context
    setup_processing_folders([])
    setup_processing_files([technical_configuration[:unknown_identifiers_file]])
  end

  def setup_processing_files(array)
    array.each do |file|
      File.delete(file) if File.exist?(file)
      File.new(file, 'w+')
    end
  end

  def setup_processing_folders(array)
    array.each do |folder|
      File.delete(file) if File.exist?(file)
      File.new(file, 'w+')
    end
  end

  def add_id_to_segment_file(id, file)
    p file
    file_key = file.chomp('.ndjson')
    segment_id = mapping_table[file_key.to_sym]
    line = {
      operation: 'UPSERT',
      user_account_id: id,
      compartment_id: technical_configuration[:compartment_id],
      segment_id: segment_id
    }
    File.write(file, "#{line.to_json}\n", mode: 'a')
  end

  def parse_file(input_file_path)
    CSV.read(input_file_path).flatten
  end

  def get_userpoint(id)
    url = "#{technical_configuration[:mics_api_url]}#{technical_configuration[:get_userpoint_endpoint]}#{id}"
    user_point_response = Faraday.get(url) do |req|
      req.headers['Authorization'] = technical_configuration[:mics_api_token]
      req.headers['Content-Type'] = 'application/json;charset=UTF-8'
      req.body = {}.to_json
    end
    res = JSON.parse(user_point_response.body)
    res['data']
  end

  def userpoint_exists?(id)
    unknown_identifiers = CSV.read(technical_configuration[:unknown_identifiers_file])
    if unknown_identifiers.include?(id)
      false
    elsif get_userpoint(id).nil?
      File.write(technical_configuration[:unknown_identifiers_file], "#{id}\n", mode: 'a')
      false
    else
      !!get_userpoint(id).find { |identifier| identifier['type'] == 'USER_POINT' }
    end
  end
end
